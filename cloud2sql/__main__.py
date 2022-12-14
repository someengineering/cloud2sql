from logging import getLogger

from resotolib.args import Namespace, ArgumentParser
from resotolib.logger import setup_logger
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from cloud2sql.collect_plugins import collect_from_plugins

# Will fail in case snowflake is not installed - which is fine.
try:
    from cloud2sql.snowflake import SnowflakeUpdater  # noqa:F401
except ImportError:
    pass

log = getLogger("cloud2sql")


def parse_args() -> Namespace:
    parser = ArgumentParser(epilog="Collect data from cloud providers and store it in a database")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--config", help="Path to config file", required=True)
    parser.add_argument(
        "--show",
        choices=["progress", "log", "none"],
        default="progress",
        help="Output to show during the process. Default: progress",
    )
    parser.add_argument(
        "--db",
        help="The database url. See https://docs.sqlalchemy.org/en/20/core/engines.html.",
        required=True,
    )
    args = parser.parse_args()
    args.log_level = "CRITICAL" if args.show != "log" else "DEBUG" if args.debug else "INFO"
    return args  # type: ignore


def collect(engine: Engine, args: Namespace) -> None:
    try:
        collect_from_plugins(engine, args)
    except Exception as e:
        log.error("Error during collection", e)
        print(f"Error syncing data to database: {e}")


def main() -> None:
    args = parse_args()
    try:
        setup_logger("cloud2sql", level=args.log_level, force=True)
        engine = create_engine(args.db)
        collect(engine, args)
    except Exception as e:
        if args.debug:  # raise exception and show complete tracelog
            raise e
        else:
            print(f"Error syncing data to database: {e}")


if __name__ == "__main__":
    main()
