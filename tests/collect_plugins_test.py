from tempfile import TemporaryDirectory

from resotolib.args import Namespace
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import Session

from cloud2sql.collect_plugins import collect_from_plugins


def test_collect() -> None:
    with TemporaryDirectory() as tmp:
        engine = create_engine("sqlite:///" + tmp + "/test.db")
        cfg = f"{tmp}/config.yml"
        with open(cfg, "w") as f:
            f.write("example: {}\n")
        collect_from_plugins(engine, Namespace(config=cfg, show="none"))
        # get all tables
        metadata = MetaData()
        metadata.reflect(bind=engine)
        assert set(metadata.tables.keys()) == {
            "example_account",
            "example_custom_resource",
            "example_instance",
            "example_network",
            "example_region",
            "example_volume",
        }
        # check that there are entries in the tables
        with Session(engine) as session:
            for table in metadata.tables.values():
                assert session.query(table).count() > 0
