from resotoclient.models import Model

from cloud2sql.parquet import ArrowModel, ArrowWriter


def test_create_schema(model: Model) -> None:
    parquet_model = ArrowModel(model)
    parquet_model.create_schema([])

    assert parquet_model.schemas.keys() == {"some_instance", "some_volume", "link_some_instance_some_volume"}
    assert set(parquet_model.schemas["some_instance"].names) == {
        "_id",
        "id",
        "cores",
        "memory",
        "name",
        "cloud",
        "account",
        "region",
        "zone",
    }
    assert set(parquet_model.schemas["some_volume"].names) == {
        "_id",
        "id",
        "capacity",
        "name",
        "cloud",
        "account",
        "region",
        "zone",
    }
    assert set(parquet_model.schemas["link_some_instance_some_volume"].names) == {"to_id", "from_id"}


def test_update(parquet_writer: ArrowWriter) -> None:

    parquet_writer.insert_node(  # type: ignore
        {
            "type": "node",
            "id": "i-123",
            "reported": {
                "kind": "some_instance",
                "id": "i-123",
                "name": "my-instance",
                "cores": 4,
                "memory": 8,
            },
            "ancestors": {
                "cloud": {"reported": {"id": "some_cloud"}},
                "account": {"reported": {"id": "some_account"}},
                "region": {"reported": {"id": "some_region"}},
                "zone": {"reported": {"id": "some_zone"}},
            },
        }
    )
    parquet_writer.insert_node(  # type: ignore
        {
            "type": "node",
            "id": "v-123",
            "reported": {
                "kind": "some_volume",
                "id": "v-123",
                "name": "my-volume",
                "capacity": 12,
            },
            "ancestors": {
                "cloud": {"reported": {"id": "some_cloud"}},
                "account": {"reported": {"id": "some_account"}},
                "region": {"reported": {"id": "some_region"}},
                "zone": {"reported": {"id": "some_zone"}},
            },
        }
    )
    parquet_writer.insert_node({"type": "edge", "from": "i-123", "to": "v-123"})  # type: ignore

    # one instance is persisted
    assert set(parquet_writer.batches["some_instance"].rows[0].values()) == {
        "i-123",
        4,
        8,
        "i-123",
        "my-instance",
        "some_cloud",
        "some_account",
        "some_region",
        "some_zone",
    }

    # one volume is persisted
    assert set(parquet_writer.batches["some_volume"].rows[0].values()) == {
        "v-123",
        12,
        "v-123",
        "my-volume",
        "some_cloud",
        "some_account",
        "some_region",
        "some_zone",
    }

    # link from instance to volume is persisted
    assert set(parquet_writer.batches["link_some_instance_some_volume"].rows[0].values()) == {"i-123", "v-123"}

    # write the batch when the batch size is reached
    parquet_writer.insert_node({"type": "edge", "from": "i-123", "to": "v-123"})  # type: ignore
    assert len(parquet_writer.batches["link_some_instance_some_volume"].rows) == 0

    # flush the batches and close the writer
    parquet_writer.close()
    assert len(parquet_writer.batches["some_instance"].rows) == 0
    assert len(parquet_writer.batches["some_volume"].rows) == 0
    assert len(parquet_writer.batches["link_some_instance_some_volume"].rows) == 0
