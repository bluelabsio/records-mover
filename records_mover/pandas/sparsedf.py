from pandas import Series, isnull, DataFrame
from math import ceil
import json
from typing import List


def compress_sparse_dataframe_columns(df: DataFrame,
                                      min_required_populated_ratio: float=0.25,
                                      mandatory: List[str]=[]):
    """Compresses a sparse dataframe by throwing sparse columns into a
    single column containing a dict representing the sparse values.

    min_required_populated_ratio: What ratio of rows must provide this
                                  data element for us to grace it with
                                  a column automatically.

    mandatory: Which columns should be provided always, even if
               there's almost no data for it.

    df:
       a    b    c    d    e    f
    0  0  NaN  NaN  NaN  NaN  NaN
    1  1  1.0  NaN  NaN  NaN  NaN
    2  2  NaN  2.0  NaN  NaN  NaN
    3  3  NaN  NaN  3.0  NaN  NaN
    4  4  NaN  NaN  NaN  4.0  NaN
    5  5  NaN  NaN  NaN  NaN  5.0

    compressed_df:
       a  compressed
    0  0          {}
    1  1  {'b': 1.0}
    2  2  {'c': 2.0}
    3  3  {'d': 3.0}
    4  4  {'e': 4.0}
    5  5  {'f': 5.0}


    After saving dataframe to Redshift:

    analytics=> select * from vbroz.compressed_test
                where json_extract_path_text(compressed, 'c') <> '';
     a | compressed
    ---+------------
     2 | {"c": 2.0}
    (1 row)

    """
    min_rows_to_save = ceil(min_required_populated_ratio * len(df.index))
    core_df = df.dropna(axis=1, how='all', thresh=min_rows_to_save)
    save_these_columns = set(mandatory) - set(core_df)
    save_these_series = {col: df[col] for col in save_these_columns}
    core_df = core_df.assign(**save_these_series)

    kept_columns = set(core_df)
    compressed_df = df.drop(kept_columns, 1)
    wordy_compressed_dicts = compressed_df.to_dict(orient='records')

    def remove_null_keys(d):
        return {k: v for k, v in d.items() if not isnull(v)}

    compressed_dicts = list(map(remove_null_keys, wordy_compressed_dicts))
    compressed_json = list(map(lambda d: json.dumps(d), compressed_dicts))

    return core_df.assign(compressed=Series(compressed_json,
                                            index=core_df.index))
