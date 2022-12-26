from resotoclient.models import Kind, Model, JsObject
from typing import Dict, List, Any, NamedTuple, Optional, Tuple, final, Literal
import pyarrow as pa
import pyarrow.csv as csv
from cloud2sql.schema_utils import (
    base_kinds,
    get_table_name,
    get_link_table_name,
    kind_properties,
    insert_node,
)
import pyarrow.parquet as pq
from pathlib import Path
from dataclasses import dataclass
import dataclasses
from abc import ABC
from copy import deepcopy
import json


class ArrowModel:
    def __init__(self, model: Model, output_format: Literal["parquet", "csv"]):
        self.model = model
        self.table_kinds = [
            kind
            for kind in model.kinds.values()
            if kind.aggregate_root and kind.runtime_kind is None and kind.fqn not in base_kinds
        ]
        self.schemas: Dict[str, pa.Schema] = {}
        self.output_format: Literal["parquet", "csv"] = output_format

    def _pyarrow_type(self, kind: str) -> pa.lib.DataType:
        if "[]" in kind:
            #  csv does not support lists
            if self.output_format == "csv":
                return pa.string()
            return pa.list_(self._pyarrow_type(kind.strip("[]")))
        elif kind.startswith("dictionary"):
            #  csv does not support dictionaries
            if self.output_format == "csv":
                return pa.string()

            (key_kind, value_kind) = kind.strip("dictionary").strip("[]").split(",")
            return pa.map_(self._pyarrow_type(key_kind.strip()), self._pyarrow_type(value_kind.strip()))
        elif kind == "int32":
            return pa.int32()
        elif kind == "int64":
            return pa.int64()
        elif kind == "float":
            pa.float32()
        elif kind == "double":
            return pa.float64()
        elif kind in {"string", "datetime", "date", "duration", "any"}:
            return pa.string()
        elif kind == "boolean":
            return pa.bool_()
        elif kind in self.model.kinds:
            #  csv does not support fancy types
            if self.output_format == "csv":
                return pa.string()

            nested_kind = self.model.kinds[kind]
            if nested_kind.runtime_kind is not None:
                return self._pyarrow_type(nested_kind.runtime_kind)

            properties, _ = kind_properties(nested_kind, self.model)
            return pa.struct(
                [
                    pa.field(p.name, self._pyarrow_type(p.kind))
                    for p in properties
                ]
            )
        else:
            raise Exception(f"Unknown kind {kind}")

    def create_schema(self, edges: List[Tuple[str, str]]) -> None:
        def table_schema(kind: Kind) -> None:
            table_name = get_table_name(kind.fqn, with_tmp_prefix=False)
            if table_name not in self.schemas:
                properties, _ = kind_properties(kind, self.model)
                schema = pa.schema(
                    [
                        pa.field("_id", pa.string()),
                        *[pa.field(p.name, self._pyarrow_type(p.kind)) for p in properties],
                    ]
                )
                self.schemas[table_name] = schema

        def link_table_schema(from_kind: str, to_kind: str) -> None:
            from_table = get_table_name(from_kind, with_tmp_prefix=False)
            to_table = get_table_name(to_kind, with_tmp_prefix=False)
            link_table = get_link_table_name(from_kind, to_kind, with_tmp_prefix=False)
            if link_table not in self.schemas and from_table in self.schemas and to_table in self.schemas:
                schema = pa.schema(
                    [
                        pa.field("from_id", pa.string()),
                        pa.field("to_id", pa.string()),
                    ]
                )
                self.schemas[link_table] = schema

        def link_table_schema_from_successors(kind: Kind) -> None:
            _, successors = kind_properties(kind, self.model)
            # create link table for all linked entities
            for successor in successors:
                link_table_schema(kind.fqn, successor)

        # step 1: create tables for all kinds
        for kind in self.table_kinds:
            table_schema(kind)
        # step 2: create link tables for all kinds
        for kind in self.table_kinds:
            link_table_schema_from_successors(kind)
        # step 3: create link tables for all seen edges
        for from_kind, to_kind in edges:
            link_table_schema(from_kind, to_kind)

        return None


class WriteResult(NamedTuple):
    table_name: str


class FileWriter(ABC):
    pass


@final
@dataclass(frozen=True)
class Parquet(FileWriter):
    parquet_writer: pq.ParquetWriter


@final
@dataclass(frozen=True)
class CSV(FileWriter):
    csv_writer: csv.CSVWriter


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


def write_batch_to_file(batch: ArrowBatch) -> ArrowBatch:

    copy = deepcopy(batch.rows)

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
    elif isinstance(batch.writer, CSV):
        batch.writer.csv_writer.close()
    else:
        raise ValueError(f"Unknown format {batch.writer}")


def new_writer(format: Literal["parquet", "csv"], table_name: str, schema: pa.Schema, result_dir: Path) -> FileWriter:
    def ensure_path(path: Path) -> Path:
        path.mkdir(parents=True, exist_ok=True)
        return path

    if format == "parquet":
        return Parquet(pq.ParquetWriter(Path(ensure_path(result_dir), f"{table_name}.parquet"), schema=schema))
    elif format == "csv":
        return CSV(csv.CSVWriter(Path(ensure_path(result_dir), f"{table_name}.csv"), schema=schema))
    else:
        raise ValueError(f"Unknown format {format}")


class ArrowWriter:
    def __init__(
        self, model: ArrowModel, result_directory: Path, rows_per_batch: int, output_format: Literal["parquet", "csv"]
    ):
        self.model = model
        self.kind_by_id: Dict[str, str] = {}
        self.batches: Dict[str, ArrowBatch] = {}
        self.rows_per_batch: int = rows_per_batch
        self.result_directory: Path = result_directory
        self.output_format: Literal["parquet", "csv"] = output_format

    def insert_value(self, table_name: str, values: Any) -> Optional[WriteResult]:
        if self.model.schemas.get(table_name):
            schema = self.model.schemas[table_name]
            batch = self.batches.get(
                table_name,
                ArrowBatch(
                    table_name,
                    [],
                    schema,
                    new_writer(
                        self.output_format, table_name, schema, self.result_directory
                    ),
                ),
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
        should_write_batch = result and len(self.batches[result.table_name].rows) > self.rows_per_batch
        if result and should_write_batch:
            batch = self.batches[result.table_name]
            self.batches[result.table_name] = write_batch_to_file(batch)

    def close(self) -> None:
        for table_name, batch in self.batches.items():
            batch = write_batch_to_file(batch)
            self.batches[table_name] = batch
            close_writer(batch)
