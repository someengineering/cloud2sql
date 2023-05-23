from typing import Iterator

from resotoclient import JsObject
from resotolib.core.actions import CoreFeedback
from resotolib.core.model_export import node_to_dict
from resotolib.graph import Graph

from cloud2sql.remote_graph import RemoteGraphCollector


def test_remote_graph_collector(example_collector_graph: Graph, core_feedback: CoreFeedback) -> None:
    def graph_iterator() -> Iterator[JsObject]:
        for node in example_collector_graph.nodes:
            fn = node_to_dict(node)
            fn["type"] = "node"
            yield fn

        for from_node, to_node in example_collector_graph.edges():
            yield {"type": "edge", "from": from_node.chksum, "to": to_node.chksum, "edge_type": "default"}

    collector = RemoteGraphCollector()
    collector.core_feedback = core_feedback
    graph_again = collector._collect_from_graph_iterator(graph_iterator())
    assert len(graph_again.nodes) == len(example_collector_graph.nodes)
    assert len(graph_again.edges) == len(example_collector_graph.edges)
