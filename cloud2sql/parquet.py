from resotoclient.models import Kind, Model, Property, JsObject
from resotolib.types import Json
from typing import Dict, Optional, Tuple, List, Any, Callable
from resotolib.baseresources import BaseResource
from resotolib import baseresources
import pyarrow as pa
import inspect
from cloud2sql.util import value_in_path
import pandas as pd
import json

# This set will hold the names of all "base" resources
# Since that are abstract classes, there will be no instances of them - hence we do not need a table for them.
base_kinds = {
    clazz.kind for _, clazz in inspect.getmembers(baseresources, inspect.isclass) if issubclass(clazz, BaseResource)
}


carz = [
    Property("cloud", "string"),
    Property("account", "string"),
    Property("region", "string"),
    Property("zone", "string"),
]
carz_access = {name: ["ancestors", name, "reported", "id"] for name in ["cloud", "account", "region", "zone"]}


class ParquetModel:
    def __init__(self, model: Model):
        self.model = model
        self.table_kinds = [
            kind
            for kind in model.kinds.values()
            if kind.aggregate_root and kind.runtime_kind is None and kind.fqn not in base_kinds
        ]
        self.schemas: Dict[str, pa.Schema] = {}

    def table_name(self, kind: str) -> str:
        replaced = kind.replace(".", "_")
        return replaced

    def link_table_name(self, from_kind: str, to_kind: str) -> str:
        # 63 characters to be consistent with SQL model
        replaced = f"link_{self.table_name(from_kind)[0:25]}_{self.table_name(to_kind)[0:25]}"
        return replaced

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
            table_name = self.table_name(kind.fqn)
            if table_name not in self.schemas:
                properties, _ = self.kind_properties(kind)
                schema = pa.schema(
                    [
                        pa.field("_id", pa.string()),
                        *[pa.field(p.name, self._parquet_type(p.kind)) for p in properties],
                    ]
                )
                self.schemas[table_name] = schema

        def link_table_schema(from_kind: str, to_kind: str) -> None:
            from_table = self.table_name(from_kind)
            to_table = self.table_name(to_kind)
            link_table = self.link_table_name(from_kind, to_kind)
            if link_table not in self.schemas and from_table in self.schemas and to_table in self.schemas:
                schema = pa.schema(
                    [
                        pa.field("from_id", pa.string()),
                        pa.field("to_id", pa.string()),
                    ]
                )
                self.schemas[link_table] = schema

        def link_table_schema_from_successors(kind: Kind) -> None:
            _, successors = self.kind_properties(kind)
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

    def kind_properties(self, kind: Kind) -> Tuple[List[Property], List[str]]:
        visited = set()

        def base_props_not_visited(kd: Kind) -> Tuple[List[Property], List[str]]:
            if kd.fqn in visited:
                return [], []
            visited.add(kd.fqn)
            # take all properties that are not synthetic
            # also ignore the kind property, since it is available in the table name
            properties: Dict[str, Property] = {
                prop.name: prop for prop in (kd.properties or []) if prop.synthetic is None and prop.name != "kind"
            }
            defaults = kd.successor_kinds.get("default") if kd.successor_kinds else None
            successors: List[str] = defaults.copy() if defaults else []
            for kind_name in kd.bases or []:
                if ck := self.model.kinds.get(kind_name):  # and kind_name != "resource":
                    props, succs = base_props_not_visited(ck)
                    for prop in props:
                        properties[prop.name] = prop
                    successors.extend(succs)
            return list(properties.values()), successors

        prs, scs = base_props_not_visited(kind)
        return prs + carz, scs


class ParquetBuilder:
    def __init__(self, model: ParquetModel):
        self.model = model
        self.kind_by_id: Dict[str, str] = {}
        self.table_content: Dict[str, List[Dict[str, Any]]] = {}

    def insert_value(self, table_name: str, values: Any) -> bool:
        if schema := self.model.schemas.get(table_name):
            table = self.table_content.get(table_name, [])
            table.append(values)
            self.table_content[table_name] = table
            return True
        return False

    def insert_node(self, node: JsObject) -> None:
        if node.get("type") == "node" and "id" in node and "reported" in node:
            reported: Json = node.get("reported", {})
            for name, value in reported.items():
                if isinstance(value, dict) or isinstance(value, list) or isinstance(value, set):
                    reported[name] = json.dumps(value)  # convert to json in case of a nested object

            reported["_id"] = node["id"]
            reported["cloud"] = value_in_path(node, carz_access["cloud"])
            reported["account"] = value_in_path(node, carz_access["account"])
            reported["region"] = value_in_path(node, carz_access["region"])
            reported["zone"] = value_in_path(node, carz_access["zone"])
            kind = reported.pop("kind")
            self.kind_by_id[node["id"]] = kind
            self.insert_value(self.model.table_name(kind), reported)
            return None
        elif node.get("type") == "edge" and "from" in node and "to" in node:
            from_id = node["from"]
            to_id = node["to"]
            if (from_kind := self.kind_by_id.get(from_id)) and (to_kind := self.kind_by_id.get(to_id)):
                link_table = self.model.link_table_name(from_kind, to_kind)
                self.insert_value(link_table, {"from_id": from_id, "to_id": to_id})
            return None
        raise ValueError(f"Unknown node: {node}")

    def to_tables(self) -> Dict[str, pa.Table]:
        result = {}
        for table_name, table in self.table_content.items():
            schema = self.model.schemas[table_name]
            result[table_name] = pa.Table.from_pylist(table, schema)
        return result
