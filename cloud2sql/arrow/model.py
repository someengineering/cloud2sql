from resotoclient.models import Kind, Model
from typing import Dict, List, Tuple, Literal
import pyarrow as pa
from cloud2sql.schema_utils import (
    base_kinds,
    get_table_name,
    get_link_table_name,
    kind_properties,
)
from cloud2sql.arrow.type_converter import parquet_pyarrow_type, csv_pyarrow_type
from functools import partial


class ArrowModel:
    def __init__(self, model: Model, output_format: Literal["parquet", "csv"]):
        self.model = model
        self.table_kinds = [
            kind
            for kind in model.kinds.values()
            if kind.aggregate_root and kind.runtime_kind is None and kind.fqn not in base_kinds
        ]
        self.schemas: Dict[str, pa.Schema] = {}
        if output_format == "parquet":
            self.pyarrow_type = partial(parquet_pyarrow_type, model=model)
        elif output_format == "csv":
            self.pyarrow_type = partial(csv_pyarrow_type, model=model)
        else:
            raise Exception(f"Unknown output format {output_format}")

    def create_schema(self, edges: List[Tuple[str, str]]) -> None:
        def table_schema(kind: Kind) -> None:
            table_name = get_table_name(kind.fqn, with_tmp_prefix=False)
            if table_name not in self.schemas:
                properties, _ = kind_properties(kind, self.model)
                schema = pa.schema(
                    [
                        pa.field("_id", pa.string()),
                        *[pa.field(p.name, self.pyarrow_type(p.kind)) for p in properties],
                    ]
                )
                self.schemas[table_name] = schema

        def link_table_schema(from_kind: str, to_kind: str) -> None:
            from_table = get_table_name(from_kind, with_tmp_prefix=False)
            to_table = get_table_name(to_kind, with_tmp_prefix=False)
            link_table = get_link_table_name(from_kind, to_kind, with_tmp_prefix=False)
            if link_table not in self.schemas and from_table in self.schemas and to_table in self.schemas:
                schema = pa.schema(
                    [
                        pa.field("from_id", pa.string()),
                        pa.field("to_id", pa.string()),
                    ]
                )
                self.schemas[link_table] = schema

        def link_table_schema_from_successors(kind: Kind) -> None:
            _, successors = kind_properties(kind, self.model)
            # create link table for all linked entities
            for successor in successors:
                link_table_schema(kind.fqn, successor)

        # step 1: create tables for all kinds
        for kind in self.table_kinds:
            table_schema(kind)
        # step 2: create link tables for all kinds
        for kind in self.table_kinds:
            link_table_schema_from_successors(kind)
        # step 3: create link tables for all seen edges
        for from_kind, to_kind in edges:
            link_table_schema(from_kind, to_kind)

        return None
