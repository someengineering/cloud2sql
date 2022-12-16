import concurrent
import multiprocessing
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, Future
import concurrent.futures
from contextlib import suppress
from logging import getLogger
from queue import Queue
from threading import Event
from time import sleep
from typing import Dict, Optional, List, Any, Tuple, Set, Literal
from pathlib import Path
from dataclasses import dataclass

import pkg_resources
import yaml
from resotoclient import Kind, Model
from resotolib.args import Namespace
from resotolib.baseplugin import BaseCollectorPlugin
from resotolib.baseresources import BaseResource, EdgeType
from resotolib.config import Config
from resotolib.core.actions import CoreFeedback
from resotolib.core.model_export import node_to_dict
from resotolib.json import from_json
from resotolib.proc import emergency_shutdown
from resotolib.types import Json
from rich import print as rich_print
from rich.live import Live
from sqlalchemy.engine import Engine


from cloud2sql.analytics import AnalyticsEventSender
from cloud2sql.show_progress import CollectInfo
from cloud2sql.sql import SqlUpdater, sql_updater

try:
    from cloud2sql.parquet import ArrowModel, ArrowWriter
except ImportError:
    pass


log = getLogger("resoto.cloud2sql")


def collectors(raw_config: Json, feedback: CoreFeedback) -> Dict[str, BaseCollectorPlugin]:
    result = {}
    config: Config = Config  # type: ignore
    for entry_point in pkg_resources.iter_entry_points("resoto.plugins"):
        plugin_class = entry_point.load()
        if issubclass(plugin_class, BaseCollectorPlugin) and plugin_class.cloud in raw_config:
            log.info(f"Found collector {plugin_class.cloud} ({plugin_class.__name__})")
            plugin_class.add_config(config)
            plugin = plugin_class()
            if hasattr(plugin, "core_feedback"):
                setattr(plugin, "core_feedback", feedback.with_context(plugin.cloud))
            result[plugin_class.cloud] = plugin

    Config.init_default_config()
    Config.running_config.data = {**Config.running_config.data, **Config.read_config(raw_config)}
    return result


@dataclass(frozen=True)
class FileDestination:
    path: Path
    format: Literal["parquet", "csv"]
    batch_size: int


def configure(path_to_config: Optional[str]) -> Json:
    def require(key: str, obj: Json, msg: str):
        if key not in obj:
            raise ValueError(msg)

    config = {}
    if path_to_config:
        with open(path_to_config) as f:
            config = yaml.safe_load(f)  # type: ignore

    if "sources" not in config:
        raise ValueError("No sources configured")
    if "destinations" not in config:
        raise ValueError("No destinations configured")

    if "file" in (config.get("destinations", {}) or {}):
        file_dest = config["destinations"]["file"]
        require("format", file_dest, "No format configured for file destination")
        if not file_dest["format"] in ["parquet", "csv"]:
            raise ValueError("Format must be either parquet or csv")
        require("path", file_dest, "No path configured for file destination")
        config["destinations"]["file"] = FileDestination(
            Path(file_dest["path"]), file_dest["format"], int(file_dest.get("batch_size", 100_000))
        )

    return config


def collect(
    collector: BaseCollectorPlugin, engine: Optional[Engine], feedback: CoreFeedback, args: Namespace, config: Json
) -> Tuple[str, int, int]:
    if engine:
        return collect_sql(collector, engine, feedback, args)
    else:
        if "file" not in config["destinations"]:
            raise ValueError("No file destination configured")
        return collect_to_file(collector, feedback, config["destinations"]["file"])


def prepare_node(node: BaseResource, collector: BaseCollectorPlugin) -> Json:
    node._graph = collector.graph
    exported = node_to_dict(node)
    exported["type"] = "node"
    exported["ancestors"] = {
        "cloud": {"reported": {"id": node.cloud().name}},
        "account": {"reported": {"id": node.account().name}},
        "region": {"reported": {"id": node.region().name}},
        "zone": {"reported": {"id": node.zone().name}},
    }
    return exported


def collect_to_file(
    collector: BaseCollectorPlugin, feedback: CoreFeedback, config: FileDestination
) -> Tuple[str, int, int]:
    # collect cloud data
    feedback.progress_done(collector.cloud, 0, 1)
    collector.collect()
    # read the kinds created from this collector
    kinds = [from_json(m, Kind) for m in collector.graph.export_model(walk_subclasses=False)]
    model = ArrowModel(Model({k.fqn: k for k in kinds}))
    node_edge_count = len(collector.graph.nodes) + len(collector.graph.edges)
    ne_current = 0
    progress_update = node_edge_count // 100
    feedback.progress_done("sync_db", 0, node_edge_count, context=[collector.cloud])

    # group all edges by kind of from/to
    edges_by_kind: Set[Tuple[str, str]] = set()
    for from_node, to_node, key in collector.graph.edges:
        if key.edge_type == EdgeType.default:
            edges_by_kind.add((from_node.kind, to_node.kind))
    # create the ddl metadata from the kinds
    model.create_schema(list(edges_by_kind))
    # ingest the data
    writer = ArrowWriter(model, config.path, config.batch_size, config.format)
    node: BaseResource
    for node in sorted(collector.graph.nodes, key=lambda n: n.kind):
        exported = prepare_node(node, collector)
        writer.insert_node(exported)
        ne_current += 1
        if ne_current % progress_update == 0:
            feedback.progress_done("sync_db", ne_current, node_edge_count, context=[collector.cloud])
    for from_node, to_node, key in collector.graph.edges:
        if key.edge_type == EdgeType.default:
            writer.insert_node({"from": from_node.chksum, "to": to_node.chksum, "type": "edge"})
            ne_current += 1
            if ne_current % progress_update == 0:
                feedback.progress_done("sync_db", ne_current, node_edge_count, context=[collector.cloud])

    writer.close()

    feedback.progress_done(collector.cloud, 1, 1)
    return collector.cloud, len(collector.graph.nodes), len(collector.graph.edges)


def collect_sql(
    collector: BaseCollectorPlugin, engine: Engine, feedback: CoreFeedback, args: Namespace
) -> Tuple[str, int, int]:
    # collect cloud data
    feedback.progress_done(collector.cloud, 0, 1)
    collector.collect()

    # group all nodes by kind
    nodes_by_kind: Dict[str, List[Json]] = defaultdict(list)
    node: BaseResource
    for node in collector.graph.nodes:
        # create an exported node with the same scheme as resotocore
        exported = prepare_node(node, collector)
        nodes_by_kind[node.kind].append(exported)

    # group all edges by kind of from/to
    edges_by_kind: Dict[Tuple[str, str], List[Json]] = defaultdict(list)
    for from_node, to_node, key in collector.graph.edges:
        if key.edge_type == EdgeType.default:
            edge_node = {"from": from_node.chksum, "to": to_node.chksum, "type": "edge"}
            edges_by_kind[(from_node.kind, to_node.kind)].append(edge_node)

    # read the kinds created from this collector
    kinds = [from_json(m, Kind) for m in collector.graph.export_model(walk_subclasses=False)]
    updater = sql_updater(Model({k.fqn: k for k in kinds}), engine)
    node_edge_count = len(collector.graph.nodes) + len(collector.graph.edges)
    ne_count = 0
    schema = f"create temp tables {engine.dialect.name}"
    syncdb = f"synchronize {engine.dialect.name}"
    feedback.progress_done(schema, 0, 1, context=[collector.cloud])
    feedback.progress_done(syncdb, 0, node_edge_count, context=[collector.cloud])
    with engine.connect() as conn:
        with conn.begin():
            # create the ddl metadata from the kinds
            updater.create_schema(conn, args, list(edges_by_kind.keys()))
            feedback.progress_done(schema, 1, 1, context=[collector.cloud])

            # insert batches of nodes by kind
            for kind, nodes in nodes_by_kind.items():
                log.info(f"Inserting {len(nodes)} nodes of kind {kind}")
                for insert in updater.insert_nodes(kind, nodes):
                    conn.execute(insert)
                ne_count += len(nodes)
                feedback.progress_done(syncdb, ne_count, node_edge_count, context=[collector.cloud])

            # insert batches of edges by from/to kind
            for from_to, nodes in edges_by_kind.items():
                log.info(f"Inserting {len(nodes)} edges from {from_to[0]} to {from_to[1]}")
                for insert in updater.insert_edges(from_to, nodes):
                    conn.execute(insert)
                ne_count += len(nodes)
                feedback.progress_done(syncdb, ne_count, node_edge_count, context=[collector.cloud])
    feedback.progress_done(collector.cloud, 1, 1)
    return collector.cloud, len(collector.graph.nodes), len(collector.graph.edges)


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
                futures.append(executor.submit(collect, collector, engine, feedback, args, raw_config))
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
