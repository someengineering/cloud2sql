from resotoclient.models import Kind, Model, JsObject
from typing import Dict, List, Any
import pyarrow as pa
from cloud2sql.schema_utils import base_kinds, get_table_name, get_link_table_name, kind_properties, insert_node

class ParquetModel:
    def __init__(self, model: Model):
        self.model = model
        self.table_kinds = [
            kind
            for kind in model.kinds.values()
            if kind.aggregate_root and kind.runtime_kind is None and kind.fqn not in base_kinds
        ]
        self.schemas: Dict[str, pa.Schema] = {}

    def _parquet_type(self, kind: str) -> pa.lib.DataType:
        if kind.startswith("dict") or "[]" in kind:
            return pa.string()  # dicts and lists are converted to json strings
        elif kind == "int32":
            return pa.int32()
        elif kind == "int64":
            return pa.int64()
        elif kind == "float":
            pa.float32()
        elif kind == "double":
            return pa.float64()
        elif kind in {"string", "datetime", "date", "duration"}:
            return pa.string()
        elif kind == "boolean":
            return pa.bool_()
        else:
            return pa.string()

    def create_schema(self) -> None:
        def table_schema(kind: Kind) -> None:
            table_name = get_table_name(kind.fqn, with_tmp_prefix=False)
            if table_name not in self.schemas:
                properties, _ = kind_properties(kind, self.model)
                schema = pa.schema(
                    [
                        pa.field("_id", pa.string()),
                        *[pa.field(p.name, self._parquet_type(p.kind)) for p in properties],
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

        return None


class ParquetBuilder:
    def __init__(self, model: ParquetModel):
        self.model = model
        self.kind_by_id: Dict[str, str] = {}
        self.table_content: Dict[str, List[Dict[str, Any]]] = {}

    def insert_value(self, table_name: str, values: Any) -> None:
        if self.model.schemas.get(table_name):
            table = self.table_content.get(table_name, [])
            table.append(values)
            self.table_content[table_name] = table
        return None

    def insert_node(self, node: JsObject) -> None:
        return insert_node(node, self.kind_by_id, self.insert_value, with_tmp_prefix=False, flatten=True)

    def to_tables(self) -> Dict[str, pa.Table]:
        result = {}
        for table_name, table in self.table_content.items():
            schema = self.model.schemas[table_name]
            result[table_name] = pa.Table.from_pylist(table, schema)
        return result
