from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from fsql.api import read_partitioned_table
from fsql.query import Q_IN, Q_OR


def make_test_dfs():
    df0 = pd.DataFrame(data={"c0": ["part0_c0_r0", "part0_c0_r1"]})
    df1 = pd.DataFrame(data={"c0": ["part1_c0_r0", "part1_c0_r1"]})
    df2 = pd.DataFrame(data={"c0": ["part2_c0_r0", "part2_c0_r1"]})
    return df0, df1, df2


def make_path(path: Path):
    path.mkdir(parents=True)
    return path


def test_q_in(tmp_path):
    data_path = tmp_path / "data_q_in"
    df0, df1, df2 = make_test_dfs()
    df0.to_csv(make_path(data_path / "part=0") / "f.csv", index=False)
    df1.to_csv(make_path(data_path / "part=1") / "f.csv", index=False)
    df2.to_csv(make_path(data_path / "part=2") / "f.csv", index=False)

    result = read_partitioned_table(f"file://{data_path}/", Q_IN("part", ["0", "1"]))
    expected = pd.concat([df0.assign(part="0"), df1.assign(part="1")])
    assert_frame_equal(result, expected)


def test_q_in_multiple(tmp_path):
    data_path = tmp_path / "data_q_in"
    df0, df1, df2 = make_test_dfs()
    df0.to_csv(make_path(data_path / "part=0") / "f.csv", index=False)
    df1.to_csv(make_path(data_path / "part=1") / "f.csv", index=False)
    df2.to_csv(make_path(data_path / "part=2") / "f.csv", index=False)

    result = read_partitioned_table(f"file://{data_path}/", Q_OR(Q_IN("part", ["0"]), Q_IN("part", ["1"])))
    expected = pd.concat([df0.assign(part="0"), df1.assign(part="1")])
    assert_frame_equal(result, expected)
