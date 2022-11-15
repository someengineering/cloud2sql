import inspect
from functools import lru_cache
from typing import List, Any, Type, Tuple, Dict, Optional

from resotoclient.models import Kind, Model, Property, JsObject
from resotolib import baseresources
from resotolib.args import Namespace
from resotolib.baseresources import BaseResource
from resotolib.types import Json
from sqlalchemy import (
    Boolean,
    Column,
    Connection,
    Double,
    Float,
    Insert,
    Integer,
    JSON,
    MetaData,
    String,
    Table,
    ValuesBase,
    DDL,
    Engine,
)
from sqlalchemy.sql.ddl import DropTable, DropConstraint
from sqlalchemy.sql.type_api import TypeEngine

from cloud2sql.util import value_in_path

# This set will hold the names of all "base" resources
# Since that are abstract classes, there will be no instances of them - hence we do not need a table for them.
base_kinds = {
    clazz.kind for _, clazz in inspect.getmembers(baseresources, inspect.isclass) if issubclass(clazz, BaseResource)
}

temp_prefix = "tmp_"

carz = [
    Property("cloud", "string"),
    Property("account", "string"),
    Property("region", "string"),
    Property("zone", "string"),
]
carz_access = {name: ["ancestors", name, "reported", "id"] for name in ["cloud", "account", "region", "zone"]}


class SqlModel:
    def __init__(self, model: Model):
        self.model = model
        self.metadata = MetaData()
        self.table_kinds = [
            kind
            for kind in model.kinds.values()
            if kind.aggregate_root and kind.runtime_kind is None and kind.fqn not in base_kinds
        ]

    @staticmethod
    def column_type_from(kind: str) -> Type[TypeEngine[Any]]:
        if "[]" in kind:
            return JSON
        elif kind.startswith("dict"):
            return JSON
        elif kind in ("int32", "int64"):
            return Integer
        elif kind in "float":
            return Float
        elif kind in "double":
            return Double
        elif kind in ("string", "date", "datetime", "duration"):
            return String
        elif kind == "boolean":
            return Boolean
        else:
            return JSON

    def table_name(self, kind: str, with_tmp_prefix: bool = True) -> str:
        replaced = kind.replace(".", "_")
        return temp_prefix + replaced if with_tmp_prefix else replaced

    def link_table_name(self, from_kind: str, to_kind: str, with_tmp_prefix: bool = True) -> str:
        # postgres table names are not allowed to be longer than 63 characters
        replaced = f"link_{self.table_name(from_kind, False)[0:25]}_{self.table_name(to_kind, False)[0:25]}"
        return temp_prefix + replaced if with_tmp_prefix else replaced

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

    def create_schema(self, connection: Connection, args: Namespace) -> MetaData:
        def table_schema(kind: Kind) -> None:
            table_name = self.table_name(kind.fqn)
            if table_name not in self.metadata.tables:
                properties, _ = self.kind_properties(kind)
                Table(
                    self.table_name(kind.fqn),
                    self.metadata,
                    *[
                        Column("_id", String, primary_key=True),
                        *[Column(p.name, self.column_type_from(p.kind)) for p in properties],
                    ],
                )

        def link_table_schema(from_kind: str, to_kind: str) -> None:
            from_table = self.table_name(from_kind)
            to_table = self.table_name(to_kind)
            link_table = self.link_table_name(from_kind, to_kind)
            if (
                link_table not in self.metadata.tables
                and from_table in self.metadata.tables
                and to_table in self.metadata.tables
            ):
                # defining a foreign key constraint on a table that does not exist yet, will fail
                # defining it on the current tmp table setup is possible, but this will not reset during table rename
                Table(link_table, self.metadata, Column("from_id", String), Column("to_id", String))

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

        # drop tables if requested
        self.metadata.drop_all(connection)

        # create the tables
        self.metadata.create_all(connection)

        return self.metadata

    @staticmethod
    def swap_temp_tables(engine: Engine) -> None:
        with engine.connect() as connection:
            metadata = MetaData()
            metadata.reflect(connection, resolve_fks=False)

            def drop_table(tl: Table) -> None:
                for cs in tl.foreign_key_constraints:
                    connection.execute(DropConstraint(cs))  # type: ignore
                connection.execute(DropTable(tl))

            for table in metadata.tables.values():
                if table.name.startswith(temp_prefix):
                    prod_table = table.name[len(temp_prefix) :]  # noqa: E203
                    if prod_table in metadata.tables:
                        drop_table(metadata.tables[prod_table])
                    connection.execute(DDL(f"ALTER TABLE {table.name} RENAME TO {prod_table}"))  # type: ignore
            # todo: create foreign key constraints on the final tables
            connection.commit()


class SqlUpdater:
    def __init__(self, model: SqlModel):
        self.model = model
        self.kind_by_id: Dict[str, str] = {}

    @lru_cache(maxsize=2048)
    def insert(self, table_name: str) -> Optional[Insert]:
        table = self.model.metadata.tables.get(table_name)
        return table.insert() if table is not None else None

    def insert_value(self, table_name: str, values: Any) -> Optional[Insert]:
        maybe_insert = self.insert(table_name)
        return maybe_insert.values(values) if maybe_insert is not None else None

    def insert_node(self, node: JsObject) -> Optional[ValuesBase]:
        if node.get("type") == "node" and "id" in node and "reported" in node:
            reported: Json = node.get("reported", {})
            reported["_id"] = node["id"]
            reported["cloud"] = value_in_path(node, carz_access["cloud"])
            reported["account"] = value_in_path(node, carz_access["account"])
            reported["region"] = value_in_path(node, carz_access["region"])
            reported["zone"] = value_in_path(node, carz_access["zone"])
            kind = reported.pop("kind")
            self.kind_by_id[node["id"]] = kind
            return self.insert_value(self.model.table_name(kind), reported)
        elif node.get("type") == "edge" and "from" in node and "to" in node:
            from_id = node["from"]
            to_id = node["to"]
            if (from_kind := self.kind_by_id.get(from_id)) and (to_kind := self.kind_by_id.get(to_id)):
                link_table = self.model.link_table_name(from_kind, to_kind)
                return self.insert_value(link_table, {"from_id": from_id, "to_id": to_id})
        raise ValueError(f"Unknown node: {node}")
