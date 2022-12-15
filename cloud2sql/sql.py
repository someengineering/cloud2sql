import inspect
import logging
from abc import ABC, abstractmethod
from typing import List, Any, Type, Tuple, Dict, Iterator

from resotoclient.models import Kind, Model, Property
from resotolib import baseresources
from resotolib.args import Namespace
from resotolib.baseresources import BaseResource
from resotolib.types import Json
from sqlalchemy import Boolean, Column, Float, Integer, JSON, MetaData, String, Table, DDL
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.sql.ddl import DropTable, DropConstraint
from sqlalchemy.sql.dml import ValuesBase

from cloud2sql.util import value_in_path

log = logging.getLogger("resoto.cloud2sql")

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


def sql_kind_to_column_type(kind_name: str, model: Model) -> Any:  # Type[TypeEngine[Any]]
    kind = model.kinds.get(kind_name)
    if "[]" in kind_name:
        return JSON
    elif kind_name.startswith("dict"):
        return JSON
    elif kind_name == "any":
        return JSON
    elif kind_name in ("int32", "int64"):
        return Integer
    elif kind_name in "float":
        return Float
    elif kind_name in "double":
        return Float  # use Double with sqlalchemy 2
    elif kind_name in ("string", "date", "datetime", "duration"):
        return String
    elif kind_name == "boolean":
        return Boolean
    elif kind and kind.properties:  # complex kind
        return JSON
    elif kind and kind.runtime_kind is not None:  # refined simple type like enum
        return sql_kind_to_column_type(kind.runtime_kind, model)
    else:
        raise ValueError(f"Not able to handle kind {kind_name}")


class SqlUpdater(ABC):
    @abstractmethod
    def insert_nodes(self, kind: str, nodes: List[Json]) -> Iterator[ValuesBase]:
        pass

    @abstractmethod
    def insert_edges(self, from_to: Tuple[str, str], nodes: List[Json]) -> Iterator[ValuesBase]:
        pass

    @abstractmethod
    def create_schema(self, connection: Connection, args: Namespace, edges: List[Tuple[str, str]]) -> MetaData:
        pass

    @staticmethod
    def swap_temp_tables(engine: Engine) -> None:
        with engine.connect() as connection:
            with connection.begin():
                metadata = MetaData()
                metadata.reflect(connection, resolve_fks=False)

                def drop_table(tl: Table) -> None:
                    for cs in tl.foreign_key_constraints:
                        connection.execute(DropConstraint(cs))
                    connection.execute(DropTable(tl))

                for table in metadata.tables.values():
                    if table.name.startswith(temp_prefix):
                        prod_table = table.name[len(temp_prefix) :]  # noqa: E203
                        if prod_table in metadata.tables:
                            drop_table(metadata.tables[prod_table])
                        connection.execute(DDL(f"ALTER TABLE {table.name} RENAME TO {prod_table}"))
                # todo: create foreign key constraints on the final tables


class SqlDefaultUpdater(SqlUpdater):
    def __init__(self, model: Model, **kwargs: Any) -> None:
        self.model = model
        self.metadata = MetaData()
        self.table_kinds = [
            kind
            for kind in model.kinds.values()
            if kind.aggregate_root and kind.runtime_kind is None and kind.fqn not in base_kinds
        ]
        self.kind_by_id: Dict[str, str] = {}
        self.column_types_fn = kwargs.get("kind_to_column_type", sql_kind_to_column_type)
        self.insert_batch_size = kwargs.get("insert_batch_size", 5000)

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

    def create_schema(self, connection: Connection, args: Namespace, edges: List[Tuple[str, str]]) -> MetaData:
        log.info(f"Create schema for {len(self.table_kinds)} kinds and their relationships")

        def table_schema(kind: Kind) -> None:
            table_name = self.table_name(kind.fqn)
            if table_name not in self.metadata.tables:
                properties, _ = self.kind_properties(kind)
                Table(
                    self.table_name(kind.fqn),
                    self.metadata,
                    *[
                        Column("_id", String, primary_key=True),
                        *[Column(p.name, self.column_types_fn(p.kind, self.model)) for p in properties],
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
        # step 3: create link tables for all seen edges
        for from_kind, to_kind in edges:
            link_table_schema(from_kind, to_kind)

        # drop tables if requested
        self.metadata.drop_all(connection)

        # create the tables
        self.metadata.create_all(connection)

        return self.metadata

    def node_to_json(self, node: Json) -> Json:
        if node.get("type") == "node" and "id" in node and "reported" in node:
            reported: Json = node.get("reported", {})
            reported["_id"] = node["id"]
            reported["cloud"] = value_in_path(node, carz_access["cloud"])
            reported["account"] = value_in_path(node, carz_access["account"])
            reported["region"] = value_in_path(node, carz_access["region"])
            reported["zone"] = value_in_path(node, carz_access["zone"])
            reported.pop("kind", None)
            return reported
        elif node.get("type") == "edge" and "from" in node and "to" in node:
            return {"from_id": node["from"], "to_id": node["to"]}
        raise ValueError(f"Unknown node: {node}")

    def insert_nodes(self, kind: str, nodes: List[Json]) -> Iterator[ValuesBase]:
        # create a defaults dict with all properties set to None
        kp, _ = self.kind_properties(self.model.kinds[kind])
        defaults = {p.name: None for p in kp}

        if (table := self.metadata.tables.get(self.table_name(kind))) is not None:
            for batch in (nodes[i : i + self.insert_batch_size] for i in range(0, len(nodes), self.insert_batch_size)):
                converted = [defaults | self.node_to_json(node) for node in batch]
                yield table.insert().values(converted)

    def insert_edges(self, from_to: Tuple[str, str], nodes: List[Json]) -> Iterator[ValuesBase]:
        from_kind, to_kind = from_to
        table = self.metadata.tables.get(self.link_table_name(from_kind, to_kind))
        maybe_insert = table.insert() if table is not None else None
        if maybe_insert is not None:
            for batch in (nodes[i : i + self.insert_batch_size] for i in range(0, len(nodes), self.insert_batch_size)):
                converted = [self.node_to_json(node) for node in batch]
                yield maybe_insert.values(converted)


# register your updater by dialect name here
DialectUpdater: Dict[str, Type[SqlUpdater]] = {}


def sql_updater(model: Model, engine: Engine) -> SqlUpdater:
    updater_class: Type[SqlUpdater] = DialectUpdater.get(engine.dialect.name, SqlDefaultUpdater)
    updater: SqlUpdater = updater_class(model)  # type: ignore
    log.info(f"Dialect {engine.dialect.name}: Use updater class {updater_class.__name__}")
    return updater
