import json

from fsql.api import read_partitioned_table
from fsql.deser import ENUMERATED_DICT_READER
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
