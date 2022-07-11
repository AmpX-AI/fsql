import logging

import pandas as pd
from pandas.testing import assert_frame_equal

from fsql.api import read_partitioned_table
from fsql.column_parser import AutoParser
from fsql.deser_dask import DaskReader
from fsql.query import Q_TRUE

logging.basicConfig(level=logging.DEBUG, force=True)
logging.getLogger("botocore").setLevel(level=logging.ERROR)


def test_dask(tmp_path):
    """Here we demonstrate a drop-in replacement of Pandas reader with a Dask reader. The underlying logic
    of Pandas reader (the default one) is to merge all files into a single dataframe, which may, at times,
    be unscalable or undesirable. Instead, Dask reader converts every single file into one partition of the
    Dataframe, in a lazy way. Therefore, `read_partitioned_table` reads just a single file to derive the
    metadata, and returns to you an object which will initiate the reading once you trigger some `compute`.
    """

    df1 = pd.DataFrame({"a": [1, 2]})
    df2 = pd.DataFrame({"a": [3, 4]})
    df3 = pd.DataFrame({"a": [5, 6]})
    pr1 = tmp_path / "c1=1"
    pr2 = tmp_path / "c1=2"
    pr3 = tmp_path / "c1=3"
    pr1.mkdir(parents=True)
    pr2.mkdir(parents=True)
    pr3.mkdir(parents=True)
    df1.to_csv(pr1 / "f1.csv", index=False)
    df2.to_csv(pr2 / "f2.csv", index=False)
    df3.to_csv(pr3 / "f3.csv", index=False)

    reader = DaskReader()
    parser = AutoParser.from_str("c1=[1,2]")
    result = read_partitioned_table(f"file://{tmp_path}/", Q_TRUE, parser, reader).compute()
    expect = pd.concat([df1.assign(c1="1"), df2.assign(c1="2")])
    assert_frame_equal(expect, result)

    # There are times when even this behaviour is not a good idea -- the first file may not give correct
    # information for metadata derivation, you may want to do some other operation on the list of delayed
    # tasks instead of concatenating to a dataframe right away, etc... In that case, you best subclass
    # the DaskReader on your own, or just use it as a starting point.
