import pyarrow as pa
from resotoclient.models import Model
from cloud2sql.schema_utils import kind_properties


def parquet_pyarrow_type(kind: str, model: Model) -> pa.lib.DataType:
    if "[]" in kind:
        return pa.list_(parquet_pyarrow_type(kind.strip("[]"), model))
    elif kind.startswith("dictionary"):
        (key_kind, value_kind) = kind.strip("dictionary").strip("[]").split(",")
        return pa.map_(parquet_pyarrow_type(key_kind.strip(), model), parquet_pyarrow_type(value_kind.strip(), model))
    elif kind == "int32":
        return pa.int32()
    elif kind == "int64":
        return pa.int64()
    elif kind == "float":
        pa.float32()
    elif kind == "double":
        return pa.float64()
    elif kind in {"string", "datetime", "date", "duration", "any"}:
        return pa.string()
    elif kind == "boolean":
        return pa.bool_()
    elif kind in model.kinds:
        nested_kind = model.kinds[kind]
        if nested_kind.runtime_kind is not None:
            return parquet_pyarrow_type(nested_kind.runtime_kind, model)

        properties, _ = kind_properties(nested_kind, model)
        return pa.struct([pa.field(p.name, parquet_pyarrow_type(p.kind, model)) for p in properties])
    else:
        raise Exception(f"Unknown kind {kind}")


def csv_pyarrow_type(kind: str, model: Model) -> pa.lib.DataType:
    if "[]" in kind:
        return pa.string()
    elif kind.startswith("dictionary"):
        return pa.string()
    elif kind == "int32":
        return pa.int32()
    elif kind == "int64":
        return pa.int64()
    elif kind == "float":
        pa.float32()
    elif kind == "double":
        return pa.float64()
    elif kind in {"string", "datetime", "date", "duration", "any"}:
        return pa.string()
    elif kind == "boolean":
        return pa.bool_()
    elif kind in model.kinds:
        return pa.string()
    else:
        raise Exception(f"Unknown kind {kind}")
