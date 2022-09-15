import json
import re

import pytest

from fsql.api import read_partitioned_table
from fsql.deser import ENUMERATED_DICT_READER, EnumeratedDictReader
from fsql.query import Q_TRUE


def test_dict_reader(helper):
    bucket = "test-bouquet"
    fs = helper.s3fs
    fs.mkdir(bucket)
    element1 = {"val": 1}
    element2 = {"val": 2}
    ser = lambda d: json.dumps(d).encode("utf-8")  # noqa: E731
    helper.put_s3_file(ser(element1), f"{bucket}/elem1.json")
    helper.put_s3_file(ser(element2), f"{bucket}/elem2.json")

    data = read_partitioned_table(f"s3://{bucket}/", Q_TRUE, data_reader=ENUMERATED_DICT_READER)

    assert data == {0: element1, 1: element2}


def test_lazy_errors(tmp_path):
    case1_path = tmp_path / "table1"
    case1_path.mkdir(parents=True)
    data1 = """{"c1": 4}"""
    data2 = """whopsie dupsie parsing oopsie"""
    with open(case1_path / "f1.json", "w") as fd:
        fd.write(data1)
    with open(case1_path / "f2.json", "w") as fd:
        fd.write(data2)

    error_line = "Expecting value: line 1 column 1 (char 0)"
    with pytest.raises(json.decoder.JSONDecodeError, match=re.escape(error_line)):
        read_partitioned_table(f"file://{case1_path}/", Q_TRUE, data_reader=ENUMERATED_DICT_READER)

    lazy_reader = EnumeratedDictReader(lazy_errors=True)
    result = read_partitioned_table(f"file://{case1_path}/", Q_TRUE, data_reader=lazy_reader)
    assert result.data == {0: json.loads(data1)}
    assert [error_line] == [e.reason for e in result.failures]
