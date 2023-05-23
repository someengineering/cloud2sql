from typing import Dict, Iterator
from typing import Optional, ClassVar

from attr import define, field
from resotoclient import ResotoClient, JsObject
from resotolib.baseplugin import BaseCollectorPlugin
from resotolib.baseresources import (
    BaseResource,
    Cloud,
    EdgeType,
    UnknownZone,
    UnknownRegion,
    UnknownAccount,
)
from resotolib.config import Config
from resotolib.core.actions import CoreFeedback
from resotolib.core.model_export import node_from_dict
from resotolib.graph import Graph
from resotolib.json import value_in_path
from resotolib.logger import log
from resotolib.types import Json


@define
class RemoteGraphConfig:
    kind: ClassVar[str] = "remote_graph"
    resoto_url: str = field(default="https://localhost:8900", metadata={"description": "URL of the resoto server"})
    psk: Optional[str] = field(default=None, metadata={"description": "Pre-shared key for the resoto server"})
    graph: str = field(default="resoto", metadata={"description": "Name of the graph to use"})
    search: Optional[str] = field(
        default=None, metadata={"description": "Search string to filter resources. None to get all resources."}
    )


carz = {"cloud": Cloud, "account": UnknownAccount, "region": UnknownRegion, "zone": UnknownZone}


class RemoteGraphCollector(BaseCollectorPlugin):
    cloud = "remote_graph"

    def __init__(self) -> None:
        super().__init__()
        self.core_feedback: Optional[CoreFeedback] = None

    @staticmethod
    def add_config(cfg: Config) -> None:
        cfg.add_config(RemoteGraphConfig)

    def collect(self) -> None:
        try:
            self.graph = self._collect_remote_graph()
        except Exception as ex:
            if self.core_feedback:
                self.core_feedback.error(f"Unhandled exception in Remote Plugin: {ex}", log)
            else:
                log.error(f"No CoreFeedback available! Unhandled exception in RemoteGraph Plugin: {ex}")
            raise

    def _collect_remote_graph(self) -> Graph:
        config: RemoteGraphConfig = Config.remote_graph
        client = ResotoClient(config.resoto_url, psk=config.psk)
        search = config.search or "is(graph_root) -[2:]->"
        return self._collect_from_graph_iterator(client.search_graph(search, graph=config.graph))

    def _collect_from_graph_iterator(self, graph_iterator: Iterator[JsObject]) -> Graph:
        assert self.core_feedback, "No CoreFeedback available!"
        graph = Graph()
        lookup: Dict[str, BaseResource] = {}
        self.core_feedback.progress_done("Remote Graph", 0, 1)

        def set_carz(jsc: Json, rs: BaseResource) -> None:
            for ancestor, clazz in carz.items():
                path = ["ancestors", ancestor, "reported"]
                if (cv := value_in_path(jsc, path)) and (cid := cv.get("id")) and (cname := cv.get("name")):
                    resource = lookup.get(cid, clazz(id=cid, name=cname))  # type: ignore
                    lookup[cid] = resource
                    setattr(rs, f"_{ancestor}", resource)  # xxx is defined by _xxx property

        for js in graph_iterator:
            if js.get("type") == "node" and isinstance(js, dict):
                node = node_from_dict(js)
                set_carz(js, node)
                lookup[js["id"]] = node
                graph.add_node(node)
            elif js.get("type") == "edge":
                if (node_from := lookup.get(js["from"])) and (node_to := lookup.get(js["to"])):
                    graph.add_edge(node_from, node_to, edge_type=EdgeType.default)
            else:
                raise ValueError(f"Unknown type: {js.get('type')}")
        self.core_feedback.progress_done("Remote Graph", 1, 1)
        return graph
