import concurrent
import multiprocessing
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, Future
from contextlib import suppress
from logging import getLogger
from queue import Queue
from threading import Event
from time import sleep
from typing import Dict, Optional, List, Any, Tuple

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


def configure(path_to_config: Optional[str]) -> Json:
    if path_to_config:
        with open(path_to_config) as f:
            return yaml.safe_load(f)  # type: ignore
    return {}


def collect(
    collector: BaseCollectorPlugin, engine: Engine, feedback: CoreFeedback, args: Namespace
) -> Tuple[str, int, int]:
    # collect cloud data
    feedback.progress_done(collector.cloud, 0, 1)
    collector.collect()

    # group all nodes by kind
    nodes_by_kind: Dict[str, List[Json]] = defaultdict(list)
    node: BaseResource
    for node in collector.graph.nodes:
        node._graph = collector.graph
        # create an exported node with the same scheme as resotocore
        exported = node_to_dict(node)
        exported["type"] = "node"
        exported["ancestors"] = {
            "cloud": {"reported": {"id": node.cloud().name}},
            "account": {"reported": {"id": node.account().name}},
            "region": {"reported": {"id": node.region().name}},
            "zone": {"reported": {"id": node.zone().name}},
        }
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


def collect_from_plugins(engine: Engine, args: Namespace, sender: AnalyticsEventSender) -> None:
    # the multiprocessing manager is used to share data between processes
    mp_manager = multiprocessing.Manager()
    core_messages: Queue[Json] = mp_manager.Queue()
    feedback = CoreFeedback("cloud2sql", "collect", "collect", core_messages)
    raw_config = configure(args.config)
    all_collectors = collectors(raw_config, feedback)
    analytics = {"total": len(all_collectors), "engine": engine.dialect.name} | {name: 1 for name in all_collectors}
    end = Event()
    with ThreadPoolExecutor(max_workers=4) as executor:
        try:
            if args.show == "progress":
                executor.submit(show_messages, core_messages, end)
            futures: List[Future[Any]] = []
            for collector in all_collectors.values():
                futures.append(executor.submit(collect, collector, engine, feedback, args))
            for future in concurrent.futures.as_completed(futures):
                name, nodes, edges = future.result()
                analytics[f"{name}_nodes"] = nodes
                analytics[f"{name}_edges"] = edges
            sender.capture("collect", **analytics)
            # when all collectors are done, we can swap all temp tables
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
