import json
from typing import Any, Optional, Dict

from resotoclient import Model
from snowflake.sqlalchemy import ARRAY, OBJECT
from sqlalchemy import Integer, Float, String, Boolean, column
from sqlalchemy import select
from sqlalchemy.sql import Insert, Values

from cloud2sql.sql import SqlDefaultUpdater, DialectUpdater


def kind_to_snowflake_type(kind_name: str, model: Model) -> Any:  # Type[TypeEngine[Any]]
    kind = model.kinds.get(kind_name)
    if "[]" in kind_name:
        return ARRAY
    elif kind_name.startswith("dict"):
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
    def __init__(self, model: Model, **args: Any) -> None:
        super().__init__(model, **args)
        self.column_types_fn = kind_to_snowflake_type

    def insert_value(self, table_name: str, values: Dict[str, Any]) -> Optional[Insert]:
        if (insert := self.insert(table_name)) is not None:
            select_array = []
            column_definitions = []
            bind_values = []
            for num, (key, value) in enumerate(values.items()):
                name = f"column{num+1}"
                select_array.append(key)
                if isinstance(value, (list, dict)):
                    column_definitions.append(column(f"parse_json({name})", is_literal=True))
                    bind_values.append(json.dumps(value))
                else:
                    column_definitions.append(column(name))
                    bind_values.append(value)

            return insert.from_select(select_array, select(Values(*column_definitions).data([bind_values])))
        return None


# register this updater for the snowflake dialect
DialectUpdater["snowflake"] = SnowflakeUpdater
