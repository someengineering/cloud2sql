import concurrent
import concurrent.futures
import multiprocessing
import re
from concurrent.futures import ThreadPoolExecutor, Future
from contextlib import suppress
from logging import getLogger
from pathlib import Path
from queue import Queue
from threading import Event
from time import sleep
from typing import Dict, Optional, List, Any, Tuple

import pkg_resources
import yaml
from resotodatalink.analytics import AnalyticsEventSender
from resotodatalink.arrow.config import ArrowOutputConfig
from resotodatalink.arrow.config import FileDestination, CloudBucketDestination, S3Bucket, GCSBucket
from resotodatalink.collect_plugins import collect_sql, collect_to_file
from resotodatalink.remote_graph import RemoteGraphCollector
from resotodatalink.show_progress import CollectInfo
from resotodatalink.sql import SqlUpdater
from resotolib.args import Namespace
from resotolib.baseplugin import BaseCollectorPlugin
from resotolib.config import Config
from resotolib.core.actions import CoreFeedback
from resotolib.json import to_json
from resotolib.proc import emergency_shutdown
from resotolib.types import Json
from rich import print as rich_print
from rich.live import Live
from sqlalchemy.engine import Engine

log = getLogger("resoto.cloud2sql")


def default_config() -> Json:
    config: Config = Config  # type: ignore
    for entry_point in pkg_resources.iter_entry_points("resoto.plugins"):
        plugin_class = entry_point.load()
        if issubclass(plugin_class, BaseCollectorPlugin):
            plugin_class.add_config(config)

    Config.init_default_config()
    return to_json(Config.running_config.data)


def collectors(raw_config: Json, feedback: CoreFeedback) -> Dict[str, BaseCollectorPlugin]:
    result = {}
    config: Config = Config  # type: ignore
    for entry_point in pkg_resources.iter_entry_points("resoto.plugins"):
        plugin_class = entry_point.load()
        if issubclass(plugin_class, BaseCollectorPlugin) and plugin_class.cloud in raw_config:
            log.info(f"Found collector {plugin_class.cloud} ({plugin_class.__name__})")
            plugin_class.add_config(config)
            plugin = plugin_class()
            result[plugin_class.cloud] = plugin

    # lookup local plugins
    if RemoteGraphCollector.cloud in raw_config:
        log.info(f"Found collector {RemoteGraphCollector.cloud} ({RemoteGraphCollector.__name__})")
        result[RemoteGraphCollector.cloud] = RemoteGraphCollector()
        RemoteGraphCollector.add_config(config)

    for plugin in result.values():
        if hasattr(plugin, "core_feedback"):
            setattr(plugin, "core_feedback", feedback.with_context(plugin.cloud))

    Config.init_default_config()
    Config.running_config.data = {**Config.running_config.data, **Config.read_config(raw_config)}
    return result


def configure(path_to_config: Optional[str]) -> Json:
    # at least one key should be present
    def require(keys: List[str], obj: Json, msg: str) -> None:
        if not (set(keys) & obj.keys()):
            raise ValueError(msg)

    config = {}
    if path_to_config:
        with open(path_to_config) as f:
            config = yaml.safe_load(f)

    if "sources" not in config:
        raise ValueError("No sources configured")
    if "destinations" not in config:
        raise ValueError("No destinations configured")

    def validate_arrow_config(config: Json) -> None:
        require(["format"], config, "No format configured for arrow destination")
        if not config["format"] in ["parquet", "csv"]:
            raise ValueError("Format must be either parquet or csv")
        require(["path", "uri"], config, "No path configured for arrow destination")

    destinations = set((config.get("destinations", {}) or {}).keys())

    if "file" in destinations:
        file_dest = config["destinations"]["file"]
        validate_arrow_config(file_dest)
        config["destinations"]["arrow"] = ArrowOutputConfig(
            destination=FileDestination(Path(file_dest["path"])),
            batch_size=int(file_dest.get("batch_size", 100_000)),
            format=file_dest["format"],
        )
        return config

    if "s3" in destinations:
        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html
        def normalize_s3_bucket_name(url: str) -> str:
            if match := re.match(r"^([^/]+)$", url):
                return match.group(1)
            if match := re.match(r"^[sS]3://([^/]+)/?.*$", url):
                return match.group(1)
            if match := re.match(r"^https://(.+)\.s3\.(.*)\.amazonaws.com/?.*$", url):
                return match.group(1)
            if match := re.match(r"^https://s3\.(.+)\.amazonaws\.com/([^/]+)/?.*", url):
                return match.group(2)
            raise ValueError(f"Unsupported S3 bucket name: {url}")

        s3_dest = config["destinations"]["s3"]
        require(["region"], s3_dest, "No region configured for s3 destination")
        validate_arrow_config(s3_dest)
        bucket_name = normalize_s3_bucket_name(s3_dest["uri"])
        config["destinations"]["arrow"] = ArrowOutputConfig(
            destination=CloudBucketDestination(
                bucket_name=bucket_name,
                cloud_bucket=S3Bucket(
                    region=s3_dest["region"],
                ),
            ),
            batch_size=int(s3_dest.get("batch_size", 100_000)),
            format=s3_dest["format"],
        )
        return config

    if "gcs" in destinations:
        gcs_dest = config["destinations"]["gcs"]
        validate_arrow_config(gcs_dest)
        bucket_name = gcs_dest["uri"].replace("gs://", "")
        config["destinations"]["arrow"] = ArrowOutputConfig(
            destination=CloudBucketDestination(
                cloud_bucket=GCSBucket(),
                bucket_name=bucket_name,
            ),
            batch_size=int(gcs_dest.get("batch_size", 100_000)),
            format=gcs_dest["format"],
        )
        return config

    return config


def collect(
    collector: BaseCollectorPlugin, engine: Optional[Engine], feedback: CoreFeedback, config: Json
) -> Tuple[str, int, int]:
    if engine:
        return collect_sql(collector, engine, feedback)
    else:
        if not {"file", "s3", "gcs"} & config["destinations"].keys():
            raise ValueError("No file destination configured")
        return collect_to_file(collector, feedback, config["destinations"]["arrow"])


def show_messages(core_messages: Queue[Json], end: Event) -> None:
    info = CollectInfo()
    while not end.is_set():
        with Live(info.render(), transient=True):
            with suppress(Exception):
                info.handle_message(core_messages.get(timeout=1))
    for message in info.rendered_messages():
        rich_print(message)


def collect_from_plugins(engine: Optional[Engine], args: Namespace, sender: AnalyticsEventSender) -> None:
    # the multiprocessing manager is used to share data between processes
    mp_manager = multiprocessing.Manager()
    core_messages: Queue[Json] = mp_manager.Queue()
    feedback = CoreFeedback("cloud2sql", "collect", "collect", core_messages)
    raw_config = configure(args.config)
    sources = raw_config["sources"]
    all_collectors = collectors(sources, feedback)
    engine_name = engine.dialect.name if engine else "file"
    analytics = {"total": len(all_collectors), "engine": engine_name} | {name: 1 for name in all_collectors}
    end = Event()
    with ThreadPoolExecutor(max_workers=4) as executor:
        try:
            if args.show == "progress":
                executor.submit(show_messages, core_messages, end)
            futures: List[Future[Any]] = []
            for collector in all_collectors.values():
                futures.append(executor.submit(collect, collector, engine, feedback, raw_config))
            for future in concurrent.futures.as_completed(futures):
                name, nodes, edges = future.result()
                analytics[f"{name}_nodes"] = nodes
                analytics[f"{name}_edges"] = edges
            sender.capture("collect", **analytics)
            # when all collectors are done, we can swap all temp tables
            if engine:
                swap_tables = "Make latest snapshot available"
                feedback.progress_done(swap_tables, 0, 1)
                SqlUpdater.swap_temp_tables(engine)
                feedback.progress_done(swap_tables, 1, 1)
        except Exception as e:
            # set end and wait for live to finish, otherwise the cursor is not reset
            end.set()
            sender.capture("error", error=str(e))
            sleep(1)
            sender.flush()
            log.error("An error occurred", exc_info=e)
            print(f"Encountered Error. Giving up. {e}")
            emergency_shutdown()
        finally:
            end.set()
