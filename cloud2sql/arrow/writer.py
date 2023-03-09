from typing import Dict, List, Any, NamedTuple, Optional, final, Union
import pyarrow.csv as csv
from dataclasses import dataclass
import dataclasses
from abc import ABC
import pyarrow.parquet as pq
import pyarrow as pa
import json
from pathlib import Path
from cloud2sql.arrow.model import ArrowModel
from cloud2sql.arrow.config import (
    ArrowOutputConfig,
    FileDestination,
    CloudBucketDestination,
    S3Bucket,
    GCSBucket,
    ArrowDestination,
)
from cloud2sql.schema_utils import insert_node
from resotoclient.models import JsObject

import boto3
from google.cloud import storage
import hashlib


class WriteResult(NamedTuple):
    table_name: str


@final
@dataclass(frozen=True)
class Parquet:
    parquet_writer: pq.ParquetWriter


@final
@dataclass(frozen=True)
class CSV:
    csv_writer: csv.CSVWriter


FileWriterFormat = Union[Parquet, CSV]


@final
@dataclass(frozen=True)
class FileWriter:
    path: Path
    format: FileWriterFormat


@final
@dataclass(frozen=True)
class ArrowBatch:
    table_name: str
    rows: List[Dict[str, Any]]
    schema: pa.Schema
    writer: FileWriter
    destination: ArrowDestination


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
    if isinstance(batch.writer.format, Parquet):
        batch.writer.format.parquet_writer.write_table(pa_table)
    elif isinstance(batch.writer.format, CSV):
        batch.writer.format.csv_writer.write_table(pa_table)
    else:
        raise ValueError(f"Unknown format {batch.writer}")
    return ArrowBatch(
        table_name=batch.table_name, rows=[], schema=batch.schema, writer=batch.writer, destination=batch.destination
    )


def close_writer(batch: ArrowBatch) -> None:
    def uploadToS3(path: Path, bucket_name: str, region: str) -> None:
        s3_client = boto3.client("s3", region_name=region)
        s3_client.upload_file(str(path), bucket_name, path.name)

    def uploadToGCS(path: Path, bucket_name: str) -> None:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(path.name)
        blob.upload_from_filename(str(path))

    def maybeUpload() -> None:
        if isinstance(batch.destination, CloudBucketDestination):
            destination = batch.destination
            if isinstance(destination.cloud_bucket, S3Bucket):
                uploadToS3(batch.writer.path, destination.bucket_name, destination.cloud_bucket.region)
            elif isinstance(destination.cloud_bucket, GCSBucket):
                uploadToGCS(batch.writer.path, destination.bucket_name)
            else:
                raise ValueError(f"Unknown cloud bucket {destination.cloud_bucket}")

    if isinstance(batch.writer.format, Parquet):
        batch.writer.format.parquet_writer.close()
        maybeUpload()
    elif isinstance(batch.writer.format, CSV):
        batch.writer.format.csv_writer.close()
        maybeUpload()
    else:
        raise ValueError(f"Unknown format {batch.writer}")


def new_writer(table_name: str, schema: pa.Schema, output_config: ArrowOutputConfig) -> FileWriter:
    def ensure_path(path: Path) -> Path:
        path.mkdir(parents=True, exist_ok=True)
        return path

    def sha(input: str) -> str:
        h = hashlib.new("sha256")
        h.update(input.encode("utf-8"))
        return h.hexdigest()

    if isinstance(output_config.destination, FileDestination):
        result_dir = ensure_path(output_config.destination.path)
    else:
        hashed_url = sha(output_config.destination.bucket_name)
        result_dir = ensure_path(Path(f"/tmp/cloud2sql-uploads/{hashed_url}"))

    file_writer_format: Union[Parquet, CSV]
    file_path: Path
    if output_config.format == "parquet":
        file_path = Path(ensure_path(result_dir), f"{table_name}.parquet")
        file_writer_format = Parquet(
            pq.ParquetWriter(file_path, schema=schema),
        )
    elif output_config.format == "csv":
        file_path = Path(ensure_path(result_dir), f"{table_name}.csv")
        file_writer_format = CSV(
            csv.CSVWriter(file_path, schema=schema),
        )
    else:
        raise ValueError(f"Unknown format {output_config.format}")

    return FileWriter(file_path, file_writer_format)


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
                    self.output_config.destination,
                )

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
