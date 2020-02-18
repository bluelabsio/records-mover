import json
import numpy as np
from ..utils.structures import map_keys, snake_to_camel, nest_dict
from pandas import DataFrame
from typing import List, Dict, Any


# http://stackoverflow.com/questions/27050108/convert-numpy-type-to-python
class NumPyJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NumPyJSONEncoder, self).default(obj)


def dataframe_to_nested_dicts(df: DataFrame,
                              to_camel: bool=False) -> List[Dict[str, Any]]:
    """
    This turns database results (expressed as a pandas dataframe) into
    potentially-nested dicts. It uses a '.' in the column names
    as hints to nest.

    e.g., the dataframe created from this query result:

    +-----+---------+--------------+
    | abc | foo.bar | foo.baz.bing |
    +-----+---------+--------------+
    |  1  | 5       | 'bazzle'     |
    +-----+---------+--------------+

    results in this dict:

    {'abc': 1, 'foo': {'bar': 5, 'baz': {'bing': 'bazzle'}}}
    """
    # 'records' output is like:
    # [{"col1": 123, "col2": "abc"}, {"col1": 456, "col2": "xyz"}]
    records = df.to_dict(orient='records')
    if to_camel:
        records = map(lambda d: map_keys(snake_to_camel, d), records)
    records = map(nest_dict, records)
    return list(records)


def json_dumps(item: str) -> Any:
    return json.dumps(item, cls=NumPyJSONEncoder)


def purge_unnamed_unused_columns(df: DataFrame) -> DataFrame:
    # Some spreadsheets will drop a trailing ',' after the
    # last column in the header with actual content when
    # exporting to CSV, leaving you with an empty column.
    # Pandas helpfully names this empty column for you as
    # "unnamed: 1", or maybe "Unnamed: 1" (not sure why/when
    # that differs).  Let's clean those up.
    for column in df:
        if column.startswith('Unnamed: ') or column.startswith('unnamed: '):
            if not df[column].notnull().any():
                df = df.drop(column, axis=1)
    return df
