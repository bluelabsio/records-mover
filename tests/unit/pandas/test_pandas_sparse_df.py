from numpy import NaN
import unittest
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from records_mover.pandas import sparsedf


class TestPandasSparseDataFrames(unittest.TestCase):
    maxDiff = None

    def test_compress_sparse_dataframe_columns(self):
        test_1 = [
            {"a": 0},
            {"a": 1, "b": 1},
            {"a": 2, "c": 2},
            {"a": 3, "d": 3},
            {"a": 4, "e": 4},
            {"a": 5, "f": 5},
        ]

        df = DataFrame.from_dict(test_1)
        compressed_df = sparsedf.compress_sparse_dataframe_columns(df)
        dicts = compressed_df.to_dict(orient='records')
        self.maxDiff = None
        self.assertListEqual(dicts,
                             [{"a": 0, "compressed": '{}'},
                              {"a": 1, "compressed": '{"b": 1.0}'},
                              {"a": 2, "compressed": '{"c": 2.0}'},
                              {"a": 3, "compressed": '{"d": 3.0}'},
                              {"a": 4, "compressed": '{"e": 4.0}'},
                              {"a": 5, "compressed": '{"f": 5.0}'}])

    def test_compress_sparse_dataframe_columns_with_mandatory(self):
        test_1 = [
            {"a": 0},
            {"a": 1, "b": 1},
            {"a": 2, "c": 2},
            {"a": 3, "d": 3},
            {"a": 4, "e": 4},
            {"a": 5, "f": 5},
        ]

        df = DataFrame.from_dict(test_1)
        compressed_df =\
            sparsedf.compress_sparse_dataframe_columns(df,
                                                       mandatory=["d", "e"])
        expected_dicts = \
            [{"a": 0, "d": NaN, "e": NaN, "compressed": '{}'},
             {"a": 1, "d": NaN, "e": NaN, "compressed": '{"b": 1.0}'},
             {"a": 2, "d": NaN, "e": NaN, "compressed": '{"c": 2.0}'},
             {"a": 3, "d": 3.0, "e": NaN, "compressed": '{}'},
             {"a": 4, "d": NaN, "e": 4.0, "compressed": '{}'},
             {"a": 5, "d": NaN, "e": NaN, "compressed": '{"f": 5.0}'}]
        expected = DataFrame.from_dict(expected_dicts)
        assert_frame_equal(compressed_df, expected, check_like=True)
