from logging import getLogger
from typing import Optional
from resotolib.args import Namespace, ArgumentParser
from resotolib.logger import setup_logger
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from cloud2sql.analytics import PosthogEventSender, NoEventSender, AnalyticsEventSender
from cloud2sql.collect_plugins import collect_from_plugins, configure
from cloud2sql.util import db_string_from_config, check_parquet_driver

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
    parser.add_argument("--config", help="Path to config file", required=True)
    parser.add_argument(
        "--show",
        choices=["progress", "log", "none"],
        default="progress",
        help="Output to show during the process. Default: progress",
    )
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
        config = configure(args.config)
        engine = None
        if next(iter(config["destinations"].keys()), None) == "file":
            check_parquet_driver()
        else:
            engine = create_engine(db_string_from_config(config))
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
