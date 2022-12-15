from logging import getLogger
from typing import Optional
from resotolib.args import Namespace, ArgumentParser
from resotolib.logger import setup_logger
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from cloud2sql.analytics import PosthogEventSender, NoEventSender, AnalyticsEventSender
from cloud2sql.collect_plugins import collect_from_plugins
from pathlib import Path

# Will fail in case snowflake is not installed - which is fine.
try:
    from cloud2sql.snowflake import SnowflakeUpdater  # noqa:F401
except ImportError:
    pass

log = getLogger("cloud2sql")


def parse_args() -> Namespace:
    parser = ArgumentParser(
        description="Collect data from cloud providers and store it in a database",
        epilog="Synchronizes cloud data to a database",
        env_args_prefix="CLOUD2SQL_",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--config", help="Path to config file", required=False)
    parser.add_argument(
        "--show",
        choices=["progress", "log", "none"],
        default="progress",
        help="Output to show during the process. Default: progress",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--db",
        help="The database url. See https://docs.sqlalchemy.org/en/20/core/engines.html.",
        required=False,
    )
    group.add_argument(
        "--parquet", type=Path, help="Switch to parquet output mode and set the directory to write parquet files to"
    )
    parser.add_argument("--parquet_batch_size", type=int, default=100_000, help="Batch rows before writing to parquet")
    parser.add_argument(
        "--analytics-opt-out",
        default=False,
        action="store_true",
        help="Do not send anonimized analytics data",
    )
    args = parser.parse_args()
    args.log_level = "CRITICAL" if args.show != "log" else "DEBUG" if args.debug else "INFO"
    return args  # type: ignore


def collect(engine: Optional[Engine], args: Namespace, sender: AnalyticsEventSender) -> None:
    try:
        collect_from_plugins(engine, args, sender)
    except Exception as e:
        log.error("Error during collection", e)
        print(f"Error syncing data to database: {e}")


def main() -> None:
    args = parse_args()
    sender: Optional[AnalyticsEventSender] = None
    try:
        setup_logger("resoto.cloud2sql", level=args.log_level, force=True)
        sender = NoEventSender() if args.analytics_opt_out else PosthogEventSender()
        engine = create_engine(args.db) if args.db else None
        collect(engine, args, sender)
    except Exception as e:
        if args.debug:  # raise exception and show complete tracelog
            raise e
        else:
            print(f"Error syncing data to database: {e}")
    finally:
        if sender:
            sender.flush()


if __name__ == "__main__":
    main()
