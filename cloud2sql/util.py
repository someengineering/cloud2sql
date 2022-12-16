from typing import Union, List, Optional, Any

from resotolib.types import JsonElement, Json
from sqlalchemy.engine import create_engine


def value_in_path(element: JsonElement, path_or_name: Union[List[str], str]) -> Optional[Any]:
    path = path_or_name if isinstance(path_or_name, list) else path_or_name.split(".")
    at = len(path)

    def at_idx(current: JsonElement, idx: int) -> Optional[Any]:
        if at == idx:
            return current
        elif current is None or not isinstance(current, dict) or path[idx] not in current:
            return None
        else:
            return at_idx(current[path[idx]], idx + 1)

    return at_idx(element, 0)


def db_string_from_config(config: Json) -> str:
    destinations = config.get("destinations", {})

    if len(destinations) != 1:
        raise ValueError("Exactly one destination must be configured")

    db_type = list(destinations.keys())[0]
    db_config = destinations[db_type]
    db_type = update_db_type(db_type)
    user = db_config.get("user")
    password = db_config.get("password")
    host = db_config.get("host")
    port = db_config.get("port")
    database = db_config.get("database")
    args = db_config.get("args", {})

    db_uri = f"{db_type}://"

    if user:
        db_uri += user
        if password:
            db_uri += f":{password}"
        db_uri += "@"

    if host:
        db_uri += host
        if port:
            db_uri += f":{port}"

    if database:
        db_uri += f"/{database}"

    if len(args) > 0:
        db_uri += "?" + "&".join([f"{k}={v}" for k, v in args.items()])

    check_db_driver(db_uri)

    return db_uri


def update_db_type(db_type: str) -> str:
    if db_type == "mysql":
        db_type = "mysql+pymysql"
    elif db_type == "mariadb":
        db_type = "mariadb+pymysql"
    return db_type


def check_db_driver(db_uri: str) -> None:
    try:
        create_engine(db_uri)
    except ModuleNotFoundError:
        err = "The database type you configured is not installed. "
        if db_uri.startswith("mysql") or db_uri.startswith("mariadb"):
            err += "Please run 'pip install cloud2sql[mysql]' and try again."
        elif db_uri.startswith("postgresql"):
            err += "Please run 'pip install cloud2sql[postgresql]' and try again."
        elif db_uri.startswith("snowflake"):
            err += "Please run 'pip install cloud2sql[snowflake]' and try again."
        elif db_uri.startswith("mssql"):
            err += "Please install the pymssql package and try again."
        elif db_uri.startswith("oracle"):
            err += "Please install the cx_oracle package and try again."
        else:
            err += "Please install the required dependencies and try again."
        raise ModuleNotFoundError(err)


def check_parquet_driver() -> None:
    try:
        import pyarrow  # noqa: F401
    except ModuleNotFoundError:
        raise ModuleNotFoundError(
            "The parquet format you configured is not installed. "
            "Please run 'pip install cloud2sql[parquet]' and try again."
        )
