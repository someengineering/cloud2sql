from tempfile import TemporaryDirectory

from resotolib.args import Namespace
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import Session

from cloud2sql.analytics import NoEventSender
from cloud2sql.collect_plugins import collect_from_plugins

import csv
import os
from pathlib import Path
import pathlib
from typing import Dict, List


def test_collect() -> None:
    with TemporaryDirectory() as tmp:
        engine = create_engine("sqlite:///" + tmp + "/test.db")
        cfg = f"{tmp}/config.yml"
        with open(cfg, "w") as f:
            f.write("sources:\n  example: {}\ndestinations:\n")
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


def test_collect_csv() -> None:
    def load_csv(path: Path) -> Dict[str, List[List[str]]]:
        """Load a folder with csv files into a dictionary"""
        result = {
            csv_path.name.strip(".csv"): list(csv.reader(open(csv_path)))[1:]
            for csv_path in pathlib.Path(path).glob("*.csv")
        }
        return result

    with TemporaryDirectory() as tmp:
        cfg = f"{tmp}/config.yml"
        csv_output = Path(f"{tmp}/csv_output")
        with open(cfg, "w") as f:
            f.write(f"sources:\n  example: {'{}'}\ndestinations:\n  file:\n    path: {csv_output}\n    format: csv")
        collect_from_plugins(None, Namespace(config=cfg, show="none"), NoEventSender())
        counts = {name: len(lines) for name, lines in load_csv(csv_output).items()}
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
        assert counts == expected_counts
