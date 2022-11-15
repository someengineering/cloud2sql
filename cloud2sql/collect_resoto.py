from resotoclient import ResotoClient
from resotolib.args import Namespace
from sqlalchemy import Engine

from cloud2sql.sql import SqlModel, SqlUpdater


def collect_from_resoto(engine: Engine, args: Namespace) -> None:
    with ResotoClient("https://localhost:8900", None) as client:
        model = SqlModel(client.model())

        updater = SqlUpdater(model)

        with engine.connect() as conn:
            model.create_schema(conn, args)
            for nd in client.search_graph("id(root) -[0:]->"):
                stmt = updater.insert_node(nd)
                if stmt is not None:
                    conn.execute(stmt)
            conn.commit()
