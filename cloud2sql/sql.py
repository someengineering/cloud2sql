from functools import lru_cache
from typing import Any, Type, Dict, Optional

from resotoclient.models import Kind, Model, JsObject
from resotolib.args import Namespace
from sqlalchemy import (
    Boolean,
    Column,
    Float,
    Integer,
    JSON,
    MetaData,
    String,
    Table,
    DDL,
)
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.sql import Insert
from sqlalchemy.sql.ddl import DropTable, DropConstraint
from sqlalchemy.sql.dml import ValuesBase
from sqlalchemy.sql.type_api import TypeEngine

from cloud2sql.schema_utils import (
    base_kinds,
    get_table_name,
    get_link_table_name,
    kind_properties,
    temp_prefix,
    insert_node,
)


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
            return Float  # use Double with sqlalchemy 2
        elif kind in ("string", "date", "datetime", "duration"):
            return String
        elif kind == "boolean":
            return Boolean
        else:
            return JSON

    def create_schema(self, connection: Connection, args: Namespace) -> MetaData:
        def table_schema(kind: Kind) -> None:
            table_name = get_table_name(kind.fqn)
            if table_name not in self.metadata.tables:
                properties, _ = kind_properties(kind, self.model)
                Table(
                    get_table_name(kind.fqn),
                    self.metadata,
                    *[
                        Column("_id", String, primary_key=True),
                        *[Column(p.name, self.column_type_from(p.kind)) for p in properties],
                    ],
                )

        def link_table_schema(from_kind: str, to_kind: str) -> None:
            from_table = get_table_name(from_kind)
            to_table = get_table_name(to_kind)
            link_table = get_link_table_name(from_kind, to_kind)
            if (
                link_table not in self.metadata.tables
                and from_table in self.metadata.tables
                and to_table in self.metadata.tables
            ):
                # defining a foreign key constraint on a table that does not exist yet, will fail
                # defining it on the current tmp table setup is possible, but this will not reset during table rename
                Table(
                    link_table,
                    self.metadata,
                    Column("from_id", String),
                    Column("to_id", String),
                )

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

        # drop tables if requested
        self.metadata.drop_all(connection)

        # create the tables
        self.metadata.create_all(connection)

        return self.metadata

    @staticmethod
    def swap_temp_tables(engine: Engine) -> None:
        with engine.connect() as connection:
            with connection.begin():
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
        return insert_node(node, self.kind_by_id, self.insert_value)
