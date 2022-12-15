from resotolib.baseresources import BaseResource
from resotolib import baseresources
import inspect
from resotoclient.models import Property, Kind, JsObject, Model
from resotolib.types import Json
from typing import List, Dict, Tuple, Optional, Any, Callable, TypeVar
from cloud2sql.util import value_in_path
import json

# This set will hold the names of all "base" resources
# Since that are abstract classes, there will be no instances of them - hence we do not need a table for them.
base_kinds = {
    clazz.kind for _, clazz in inspect.getmembers(baseresources, inspect.isclass) if issubclass(clazz, BaseResource)
}

temp_prefix = "tmp_"

carz = [
    Property("cloud", "string"),
    Property("account", "string"),
    Property("region", "string"),
    Property("zone", "string"),
]
carz_access = {name: ["ancestors", name, "reported", "id"] for name in ["cloud", "account", "region", "zone"]}


def get_table_name(kind: str, with_tmp_prefix: bool = True) -> str:
    replaced = kind.replace(".", "_")
    return temp_prefix + replaced if with_tmp_prefix else replaced


def get_link_table_name(from_kind: str, to_kind: str, with_tmp_prefix: bool = True) -> str:
    # postgres table names are not allowed to be longer than 63 characters
    replaced = f"link_{get_table_name(from_kind, False)[0:25]}_{get_table_name(to_kind, False)[0:25]}"
    return temp_prefix + replaced if with_tmp_prefix else replaced


def kind_properties(kind: Kind, model: Model) -> Tuple[List[Property], List[str]]:
    visited = set()

    def base_props_not_visited(kd: Kind) -> Tuple[List[Property], List[str]]:
        if kd.fqn in visited:
            return [], []
        visited.add(kd.fqn)
        # take all properties that are not synthetic
        # also ignore the kind property, since it is available in the table name
        properties: Dict[str, Property] = {
            prop.name: prop for prop in (kd.properties or []) if prop.synthetic is None and prop.name != "kind"
        }
        defaults = kd.successor_kinds.get("default") if kd.successor_kinds else None
        successors: List[str] = defaults.copy() if defaults else []
        for kind_name in kd.bases or []:
            if ck := model.kinds.get(kind_name):  # and kind_name != "resource":
                props, succs = base_props_not_visited(ck)
                for prop in props:
                    properties[prop.name] = prop
                successors.extend(succs)
        return list(properties.values()), successors

    prs, scs = base_props_not_visited(kind)
    return prs + carz, scs


T = TypeVar("T")


def insert_node(
    node: JsObject,
    kind_by_id: Dict[str, str],
    insert_value: Callable[[str, Any], Optional[T]],
    with_tmp_prefix: bool = True,
    flatten: bool = False,
) -> Optional[T]:
    if node.get("type") == "node" and "id" in node and "reported" in node:
        reported: Json = node.get("reported", {})
        if flatten:
            for name, value in reported.items():
                if isinstance(value, (dict, list, set)):
                    reported[name] = json.dumps(value)  # convert to json in case of a nested object

        reported["_id"] = node["id"]
        reported["cloud"] = value_in_path(node, carz_access["cloud"])
        reported["account"] = value_in_path(node, carz_access["account"])
        reported["region"] = value_in_path(node, carz_access["region"])
        reported["zone"] = value_in_path(node, carz_access["zone"])
        kind = reported.pop("kind")
        kind_by_id[node["id"]] = kind
        return insert_value(get_table_name(kind, with_tmp_prefix), reported)
    elif node.get("type") == "edge" and "from" in node and "to" in node:
        from_id = node["from"]
        to_id = node["to"]
        if (from_kind := kind_by_id.get(from_id)) and (to_kind := kind_by_id.get(to_id)):
            link_table = get_link_table_name(from_kind, to_kind, with_tmp_prefix)
            return insert_value(link_table, {"from_id": from_id, "to_id": to_id})
    raise ValueError(f"Unknown node: {node}")
