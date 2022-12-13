from resotoclient import ResotoClient
from resotolib.args import Namespace
from sqlalchemy.engine import Engine

from cloud2sql.sql import sql_updater


def collect_from_resoto(engine: Engine, args: Namespace) -> None:
    with ResotoClient("https://localhost:8900", None) as client:
        updater = sql_updater(client.model(), engine)

        with engine.connect() as conn:
            with conn.begin():
                updater.create_schema(conn, args)
                for nd in client.search_graph("id(root) -[0:]->"):
                    stmt = updater.insert_node(nd)
                    if stmt is not None:
                        conn.execute(stmt)
