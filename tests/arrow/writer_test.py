from cloud2sql.arrow.writer import collect_normalization_paths, normalize, ParquetMap, ParquetString, NormalizationPath
import pyarrow as pa


def test_create_collect_paths() -> None:
    schema = pa.schema(
        [
            pa.field("str", pa.string()),
            pa.field("int", pa.int32()),
            pa.field("str_list", pa.list_(pa.string())),
            pa.field("str_list_struct", pa.list_(pa.struct([pa.field("str", pa.string())]))),
            pa.field("str_dict", pa.map_(pa.string(), pa.string())),
            pa.field("int_list", pa.list_(pa.int32())),
            pa.field("struct", pa.struct([pa.field("str", pa.string())])),
            pa.field("struct_dict", pa.struct([pa.field("str", pa.string())])),
        ]
    )

    paths = collect_normalization_paths(schema)
    assert paths == [
        NormalizationPath(path=["str"], convert_to=ParquetString()),
        NormalizationPath(path=["str_list", None], convert_to=ParquetString()),
        NormalizationPath(path=["str_list_struct", None, "str"], convert_to=ParquetString()),
        NormalizationPath(path=["str_dict"], convert_to=ParquetMap(convert_values_to_str=True)),
        NormalizationPath(path=["struct", "str"], convert_to=ParquetString()),
        NormalizationPath(path=["struct_dict", "str"], convert_to=ParquetString()),
    ]


def test_normalize() -> None:
    object = {
        "foo": "bar",
        "baz": {"a": "b", "c": "d"},
        "bar": [{"a": "b", "c": "d"}, {"a": "b", "c": "d"}],
        "foobar": {"a": "b", "c": "d"},
    }

    normalize(NormalizationPath(path=["baz"], convert_to=ParquetMap(convert_values_to_str=True)), object)

    assert object["baz"] == [("a", "b"), ("c", "d")]

    normalize(NormalizationPath(path=["bar", None], convert_to=ParquetMap(convert_values_to_str=True)), object)

    assert object["bar"] == [[("a", "b"), ("c", "d")], [("a", "b"), ("c", "d")]]  # type: ignore

    normalize(NormalizationPath(path=["foobar"], convert_to=ParquetString()), object)

    assert object["foobar"] == '{"a": "b", "c": "d"}'
