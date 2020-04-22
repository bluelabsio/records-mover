import json
import numpy as np
from pandas import DataFrame
from typing import Any


# http://stackoverflow.com/questions/27050108/convert-numpy-type-to-python
class NumPyJSONEncoder(json.JSONEncoder):
    def default(self, obj: object) -> object:
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NumPyJSONEncoder, self).default(obj)


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
