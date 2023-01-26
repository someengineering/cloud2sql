from typing import Dict, List, Any, NamedTuple, Optional, final
import pyarrow.csv as csv
from dataclasses import dataclass
import dataclasses
from abc import ABC
import pyarrow.parquet as pq
import pyarrow as pa
import json
from pathlib import Path
from cloud2sql.arrow.model import ArrowModel
from cloud2sql.arrow import ArrowOutputConfig, S3Destination, GCSDestination, FileDestination
from cloud2sql.schema_utils import insert_node
from resotoclient.models import JsObject
from pyarrow import fs


class WriteResult(NamedTuple):
    table_name: str


class FileWriter(ABC):
    pass


@final
@dataclass(frozen=True)
class Parquet(FileWriter):
    parquet_writer: pq.ParquetWriter
    output_stream: pa.PythonFile


@final
@dataclass(frozen=True)
class CSV(FileWriter):
    csv_writer: csv.CSVWriter
    output_stream: pa.PythonFile


@final
@dataclass
class ArrowBatch:
    table_name: str
    rows: List[Dict[str, Any]]
    schema: pa.Schema
    writer: FileWriter


class ConversionTarget(ABC):
    pass


@final
@dataclass(frozen=True)
class ParquetMap(ConversionTarget):
    convert_values_to_str: bool


@final
@dataclass(frozen=True)
class ParquetString(ConversionTarget):
    pass


@dataclass
class NormalizationPath:
    path: List[Optional[str]]
    convert_to: ConversionTarget


# workaround until fix is merged https://issues.apache.org/jira/browse/ARROW-17832
#
# here we collect the paths to the JSON object fields that we want to convert to arrow types
# so that later we will do the transformations
#
# currently we do dict -> list[(key, value)] and converting values which are defined as strings
# in the scheme to strings/json strings
def collect_normalization_paths(schema: pa.Schema) -> List[NormalizationPath]:
    paths: List[NormalizationPath] = []

    def collect_paths_to_maps_helper(path: List[Optional[str]], typ: pa.DataType) -> None:
        # if we see a map, then full stop. we add the path to the list
        # if the value type is string, we remember that too
        if isinstance(typ, pa.lib.MapType):
            stringify_items = pa.types.is_string(typ.item_type)
            normalization_path = NormalizationPath(path, ParquetMap(stringify_items))
            paths.append(normalization_path)
        # structs are traversed but they have no interest for us
        elif isinstance(typ, pa.lib.StructType):
            for field_idx in range(0, typ.num_fields):
                field = typ.field(field_idx)
                collect_paths_to_maps_helper(path + [field.name], field.type)
        # the lists traversed too. None will is added to the path to be consumed by the recursion
        # in order to reach the correct level
        elif isinstance(typ, pa.lib.ListType):
            collect_paths_to_maps_helper(path + [None], typ.value_type)
        # if we see a string, then we stop and add a path to the list
        elif pa.types.is_string(typ):
            normalization_path = NormalizationPath(path, ParquetString())
            paths.append(normalization_path)

    # bootstrap the recursion
    for idx, typ in enumerate(schema.types):
        collect_paths_to_maps_helper([schema.names[idx]], typ)

    return paths


def normalize(npath: NormalizationPath, obj: Any) -> Any:
    path = npath.path
    reached_target = len(path) == 0

    # we're on the correct node, time to convert it into something
    if reached_target:
        if isinstance(npath.convert_to, ParquetString):
            # everything that should be a string is either a string or a json string
            return obj if isinstance(obj, str) else json.dumps(obj)
        elif isinstance(npath.convert_to, ParquetMap):
            # we can only convert dicts to maps. if it is not the case then it is a bug
            if not isinstance(obj, dict):
                raise Exception(f"Expected dict, got {type(obj)}. path: {npath}")

            def value_to_string(v: Any) -> str:
                if isinstance(v, str):
                    return v
                else:
                    return json.dumps(v)

            # in case the map should contain string values, we convert them to strings
            if npath.convert_to.convert_values_to_str:
                return [(k, value_to_string(v)) for k, v in obj.items()]
            else:
                return list(obj.items())
    # we're not at the target node yet, so we traverse the tree deeper
    else:
        # if we see a dict, we try to go deeper in case it contains the key we are looking for
        # otherwise we return the object as is. This is valid because the fields are optional
        if isinstance(obj, dict):
            key = path[0]
            if key in obj:
                # consume the current element of the path
                new_npath = dataclasses.replace(npath, path=path[1:])
                obj[key] = normalize(new_npath, obj[key])
            return obj
        # in case of a list, we process all its elements
        elif isinstance(obj, list):
            # check that the path is correct
            assert path[0] is None
            # consume the current element of the path
            new_npath = dataclasses.replace(npath, path=path[1:])
            return [normalize(new_npath, v) for v in obj]
        else:
            raise Exception(f"Unexpected object type {type(obj)}, path: {npath}")


def write_batch_to_file(batch: ArrowBatch) -> ArrowBatch:

    to_normalize = collect_normalization_paths(batch.schema)

    for row in batch.rows:
        for path in to_normalize:
            normalize(path, row)

    pa_table = pa.Table.from_pylist(batch.rows, batch.schema)
    if isinstance(batch.writer, Parquet):
        batch.writer.parquet_writer.write_table(pa_table)
    elif isinstance(batch.writer, CSV):
        batch.writer.csv_writer.write_table(pa_table)
    else:
        raise ValueError(f"Unknown format {batch.writer}")
    return ArrowBatch(table_name=batch.table_name, rows=[], schema=batch.schema, writer=batch.writer)


def close_writer(batch: ArrowBatch) -> None:
    if isinstance(batch.writer, Parquet):
        batch.writer.parquet_writer.close()
        batch.writer.output_stream.close()
    elif isinstance(batch.writer, CSV):
        batch.writer.csv_writer.close()
        batch.writer.output_stream.close()
    else:
        raise ValueError(f"Unknown format {batch.writer}")


def new_writer(table_name: str, schema: pa.Schema, output_config: ArrowOutputConfig) -> FileWriter:
    def ensure_path(path: Path) -> Path:
        path.mkdir(parents=True, exist_ok=True)
        return path

    filesystem: fs.FileSystem
    uri: str

    if isinstance(output_config.destination, FileDestination):
        filesystem = fs.LocalFileSystem()
        parent_dir = ensure_path(output_config.destination.path)
        uri = str(parent_dir)
    elif isinstance(output_config.destination, S3Destination):
        filesystem = fs.S3FileSystem(region=output_config.destination.region)
        uri = output_config.destination.uri
        if uri.startswith("s3://"):
            uri = uri[5:]
    elif isinstance(output_config.destination, GCSDestination):
        filesystem = fs.GCSFileSystem()
        uri = output_config.destination.uri
        if uri.startswith("gcs://"):
            uri = uri[6:]
    else:
        raise ValueError(f"Unknown filesystem type {type(output_config.destination)}")

    if output_config.format == "parquet":
        output_stream = filesystem.open_output_stream(f"{uri}/{table_name}.parquet")
        return Parquet(pq.ParquetWriter(output_stream, schema=schema), output_stream)
    elif output_config.format == "csv":
        output_stream = filesystem.open_output_stream(f"{uri}/{table_name}.csv")
        return CSV(csv.CSVWriter(output_stream, schema=schema), output_stream)
    else:
        raise ValueError(f"Unknown format {output_config.format}")


class ArrowWriter:
    def __init__(self, model: ArrowModel, output_config: ArrowOutputConfig):
        self.model = model
        self.kind_by_id: Dict[str, str] = {}
        self.batches: Dict[str, ArrowBatch] = {}
        self.output_config: ArrowOutputConfig = output_config

    def insert_value(self, table_name: str, values: Any) -> Optional[WriteResult]:
        if self.model.schemas.get(table_name):
            schema = self.model.schemas[table_name]
            if table_name in self.batches:
                batch = self.batches[table_name]
            else:
                batch = ArrowBatch(
                    table_name,
                    [],
                    schema,
                    new_writer(table_name, schema, self.output_config),
                )

            # batch = self.batches.get(
            #     table_name,
            #     ArrowBatch(
            #         table_name,
            #         [],
            #         schema,
            #         new_writer(table_name, schema, self.output_config),
            #     ),
            # )

            batch.rows.append(values)
            self.batches[table_name] = batch
            return WriteResult(table_name)
        return None

    def insert_node(self, node: JsObject) -> None:
        result = insert_node(
            node,
            self.kind_by_id,
            self.insert_value,
            with_tmp_prefix=False,
        )
        should_write_batch = result and len(self.batches[result.table_name].rows) > self.output_config.batch_size
        if result and should_write_batch:
            batch = self.batches[result.table_name]
            self.batches[result.table_name] = write_batch_to_file(batch)

    def close(self) -> None:
        for table_name, batch in self.batches.items():
            batch = write_batch_to_file(batch)
            self.batches[table_name] = batch
            close_writer(batch)
