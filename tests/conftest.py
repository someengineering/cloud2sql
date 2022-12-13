from queue import Queue
from typing import List

from resotoclient.models import Model, Kind, Property
from pytest import fixture
from resotolib.args import Namespace
from resotolib.core.actions import CoreFeedback
from resotolib.types import Json
from sqlalchemy.engine import create_engine, Engine

from cloud2sql.sql import SqlModel, SqlUpdater


@fixture
def model() -> Model:
    kinds: List[Kind] = [
        Kind("string", "str", None, None),
        Kind("int32", "int32", None, None),
        Kind("int64", "int64", None, None),
        Kind("float", "float", None, None),
        Kind("double", "double", None, None),
        Kind("boolean", "boolean", None, None),
        Kind(
            "resource",
            runtime_kind=None,
            properties=[
                Property("id", "string"),
                Property("name", "string"),
            ],
            bases=[],
            aggregate_root=True,
        ),
        Kind(
            "some_instance",
            runtime_kind=None,
            properties=[
                Property("cores", "int32"),
                Property("memory", "int64"),
            ],
            bases=["resource"],
            aggregate_root=True,
            successor_kinds={"default": ["some_volume"]},
        ),
        Kind(
            "some_volume",
            runtime_kind=None,
            properties=[
                Property("capacity", "int32"),
            ],
            bases=["resource"],
            aggregate_root=True,
        ),
    ]
    return Model({k.fqn: k for k in kinds})


@fixture()
def args() -> Namespace:
    return Namespace()


@fixture()
def sql_model(model: Model) -> SqlModel:
    return SqlModel(model)


@fixture()
def updater(sql_model: SqlModel) -> SqlUpdater:
    return SqlUpdater(sql_model)


@fixture
def engine() -> Engine:
    return create_engine("sqlite:///:memory:")


@fixture
def engine_with_schema(model: Model, sql_model: SqlModel, args: Namespace) -> Engine:
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as connection:
        sql_model.create_schema(connection, args)
    return engine


@fixture
def feedback_queue() -> Queue[Json]:
    return Queue()


@fixture
def core_feedback(feedback_queue: Queue[Json]) -> CoreFeedback:
    return CoreFeedback("cloud2sql", "test", "collect", feedback_queue)
