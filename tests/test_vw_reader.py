import re

import pytest
from pyrsistent import pvector

from fsql.api import read_partitioned_table
from fsql.deser_vw import Feature, FeatureNamespace, VwParsingError, VwReader, VwRow
from fsql.query import Q_TRUE


def test_vw_reader(helper):
    bucket = "test-bouquet"
    fs = helper.s3fs
    fs.mkdir(bucket)
    elements = [
        b"1 1.5 tag|ns1:1.0 f1 f2| f3 f4",
        b"0 | f3 f4",
        b"1 2.0 'tag | f5:4.7 f6:2.5",
    ]
    for i, e in enumerate(elements):
        helper.put_s3_file(e, f"{bucket}/elem{i}.vw")

    data = read_partitioned_table(f"s3://{bucket}/", Q_TRUE, data_reader=VwReader())
    expected = pvector(
        [
            VwRow(
                label=1.0,
                importance=1.5,
                tag="tag",
                features={
                    FeatureNamespace("ns1", 1.0): [Feature("f1", None), Feature("f2", None)],
                    FeatureNamespace("", 1.0): [Feature("f3", None), Feature("f4", None)],
                },
            ),
            VwRow(
                label=0.0,
                importance=1.0,
                tag=None,
                features={FeatureNamespace("", 1.0): [Feature("f3", None), Feature("f4", None)]},
            ),
            VwRow(
                label=1.0,
                importance=2.0,
                tag="tag",
                features={FeatureNamespace("", 1.0): [Feature("f5", 4.7), Feature("f6", 2.5)]},
            ),
        ]
    )
    assert data == expected


def test_lazy_errors(tmp_path):
    case1_path = tmp_path / "table1"
    case1_path.mkdir(parents=True)
    data = [
        "0 | f3 f4",
        "1 2.3 3.2 | f1 f2",
    ]
    for i, e in enumerate(data):
        with open(case1_path / f"f{i}.vw", "w") as fd:
            fd.write(e)

    error_line = "unparseable header_data=['2.3', '3.2'], header_raw='1 2.3 3.2 '"
    with pytest.raises(VwParsingError, match=re.escape(error_line)):
        read_partitioned_table(f"file://{case1_path}/", Q_TRUE, data_reader=VwReader())

    lazy_reader = VwReader(lazy_errors=True)
    result = read_partitioned_table(f"file://{case1_path}/", Q_TRUE, data_reader=lazy_reader)
    assert result.data == pvector(
        [
            VwRow(
                label=0.0,
                importance=1.0,
                tag=None,
                features={FeatureNamespace("", 1.0): [Feature("f3", None), Feature("f4", None)]},
            ),
        ]
    )
    assert [error_line] == [e.reason for e in result.failures]
