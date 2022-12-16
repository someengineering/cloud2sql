import json
import logging
from typing import Any, List, Iterator

from resotoclient import Model
from resotoclient.models import Property
from resotolib.types import Json
from snowflake.sqlalchemy import ARRAY, OBJECT
from sqlalchemy import Integer, Float, String, Boolean, column
from sqlalchemy import select
from sqlalchemy.sql import Values
from sqlalchemy.sql.dml import ValuesBase

from cloud2sql.sql import SqlDefaultUpdater, DialectUpdater

from cloud2sql.schema_utils import kind_properties, get_table_name

log = logging.getLogger("resoto.cloud2sql.snowflake")


def kind_to_snowflake_type(kind_name: str, model: Model) -> Any:  # Type[TypeEngine[Any]]
    """
    Map internal kinds to snowflake types.
    More or less the default mapping, but with some special cases for OBJECT and ARRAY types.
    """
    kind = model.kinds.get(kind_name)
    if "[]" in kind_name:
        return ARRAY
    elif kind_name.startswith("dict"):
        return OBJECT
    elif kind_name == "any":
        return OBJECT
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
    elif kind.runtime_kind is not None:  # refined simple type like enum
        return kind_to_snowflake_type(kind.runtime_kind, model)
    elif kind.properties:  # complex kind
        return OBJECT
    else:
        raise ValueError(f"Not able to handle kind {kind_name}")


class SnowflakeUpdater(SqlDefaultUpdater):
    """
    This updater synchronizes resource data to snowflake https://www.snowflake.com
    Snowflake needs special handling, since it does not support default json or array types.
    It also does not understand json or array types as bind parameters.
    This updater handles those shortcomings by using special insert statements.
    """

    def __init__(self, model: Model, **args: Any) -> None:
        super().__init__(model, **args)
        self.column_types_fn = kind_to_snowflake_type

    def insert_nodes(self, kind: str, nodes: List[Json]) -> Iterator[ValuesBase]:
        kp, _ = kind_properties(self.model.kinds[kind], self.model)
        kind_props = [Property("_id", "string")] + kp
        select_array = []
        column_definitions = []
        prop_is_json = {}

        # Inserting structured data into Snowflake requires a bit of work. General scheme:
        # insert into TBL(col_string, col_json) SELECT column1, parse_json(column2) from values('a', '{"b":1}');
        # All json and array elements need to be json encoded and parsed on the server side again.
        for num, prop in enumerate(kind_props):
            name = f"column{num+1}"
            select_array.append(prop.name)
            snowflake_kind = kind_to_snowflake_type(prop.kind, self.model)
            if snowflake_kind in (ARRAY, OBJECT):
                column_definitions.append(column(f"parse_json({name})", is_literal=True))
                prop_is_json[prop.name] = True
            else:
                column_definitions.append(column(name))

        def values_tuple(node: Json) -> List[Any]:
            nj = self.node_to_json(node)
            # make sure to use the same order as in select_array
            return [json.dumps(nj.get(p.name)) if prop_is_json.get(p.name) else nj.get(p.name) for p in kind_props]

        if (table := self.metadata.tables.get(get_table_name(kind))) is not None:
            for batch in (nodes[i : i + self.insert_batch_size] for i in range(0, len(nodes), self.insert_batch_size)):
                converted = [values_tuple(node) for node in batch]
                yield table.insert().from_select(select_array, select(Values(*column_definitions).data(converted)))


# register this updater for the snowflake dialect, when snowflake is installed
DialectUpdater["snowflake"] = SnowflakeUpdater
