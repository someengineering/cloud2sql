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
from abc import ABC
from copy import deepcopy


class ArrowModel:
    def __init__(self, model: Model):
        self.model = model
        self.table_kinds = [
            kind
            for kind in model.kinds.values()
            if kind.aggregate_root and kind.runtime_kind is None and kind.fqn not in base_kinds
        ]
        self.schemas: Dict[str, pa.Schema] = {}

    def _pyarrow_type(self, kind: str) -> pa.lib.DataType:
        if "[]" in kind:
            return pa.list_(self._pyarrow_type(kind.strip("[]")))
        elif kind.startswith("dictionary"):
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
    rows: List[Dict[str, Any]]
    schema: pa.Schema
    writer: FileWriter


def write_batch_to_file(batch: ArrowBatch) -> ArrowBatch:

    copy = deepcopy(batch.rows)

    # workaround until fix is merged https://issues.apache.org/jira/browse/ARROW-17832
    def collect_paths_to_maps(schema: pa.Schema) -> List[List[str]]:
        paths: List[List[str]] = []

        def collect_paths_to_maps_helper(path: List[str], typ: pa.DataType) -> None:
            if isinstance(typ, pa.lib.MapType):
                paths.append(path)
                collect_paths_to_maps_helper(path, typ.item_type)
            elif isinstance(typ, pa.lib.StructType):
                for field_idx in range(0, typ.num_fields):
                    field = typ.field(field_idx)
                    collect_paths_to_maps_helper(path + [field.name], field.type)
            elif isinstance(typ, pa.lib.ListType):
                collect_paths_to_maps_helper(path, typ.value_type)

        for idx, typ in enumerate(schema.types):
            collect_paths_to_maps_helper([schema.names[idx]], typ)

        return paths

    def convert_dict(path: List[str], obj: Any) -> Any:
        if isinstance(obj, dict):
            if len(path) == 0:
                return list(obj.items())
            else:
                key = path[0]
                if key in obj:
                    obj[key] = convert_dict(path[1:], obj[key])
                return obj
        elif isinstance(obj, list):
            return [convert_dict(path, v) for v in obj]
        else:
            return obj

    paths_to_maps = collect_paths_to_maps(batch.schema)

    for row in batch.rows:
        for path in paths_to_maps:
            convert_dict(path, row)

    pa_table = pa.Table.from_pylist(batch.rows, batch.schema)
    if isinstance(batch.writer, Parquet):
        batch.writer.parquet_writer.write_table(pa_table)
    elif isinstance(batch.writer, CSV):
        batch.writer.csv_writer.write_table(pa_table)
    else:
        raise ValueError(f"Unknown format {batch.writer}")
    return ArrowBatch(rows=[], schema=batch.schema, writer=batch.writer)


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
