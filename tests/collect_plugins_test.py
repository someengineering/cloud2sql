from tempfile import TemporaryDirectory

from resotolib.args import Namespace
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import Session

from cloud2sql.analytics import NoEventSender
from cloud2sql.collect_plugins import collect_from_plugins


def test_collect() -> None:
    with TemporaryDirectory() as tmp:
        engine = create_engine("sqlite:///" + tmp + "/test.db")
        cfg = f"{tmp}/config.yml"
        with open(cfg, "w") as f:
            f.write("example: {}\n")
        collect_from_plugins(engine, Namespace(config=cfg, show="none"), NoEventSender())
        # get all tables
        metadata = MetaData()
        metadata.reflect(bind=engine)
        expected_counts = {
            "example_account": 1,
            "example_custom_resource": 1,
            "example_instance": 2,
            "example_network": 2,
            "example_region": 2,
            "example_volume": 2,
            "link_example_account_example_region": 2,
            "link_example_instance_example_volume": 2,
            "link_example_network_example_instance": 2,
            "link_example_region_example_custom_resource": 1,
            "link_example_region_example_instance": 2,
            "link_example_region_example_network": 2,
            "link_example_region_example_volume": 2,
        }

        assert set(metadata.tables.keys()) == expected_counts.keys()  # check that there are entries in the tables
        with Session(engine) as session:
            for table in metadata.tables.values():
                assert session.query(table).count() == expected_counts[table.name]
