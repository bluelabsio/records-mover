"""Sample data and comparison helpers."""
import datetime as dt
from decimal import Decimal

import pandas as pd

SMALL = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['alice', 'bob', "o'brien, \"quoted\""],
    'score': [0.5, None, 3.25],
    'active': [True, False, True],
    'updated_at': pd.to_datetime(['2024-01-01 12:00:00', '2024-06-30 23:59:59',
                                  '2024-12-31 00:00:01']),
})

# Columns of the pandas `object` dtype: a known type-fidelity gap (lands as varchar)
OBJECT_TYPES = pd.DataFrame({
    'id': [1, 2, 3],
    'amount': [Decimal('1.50'), Decimal('20.25'), None],
    'signup_date': [dt.date(2024, 1, 1), dt.date(2024, 2, 29), None],
})

INT_TYPES = {'integer', 'bigint', 'smallint'}
STRING_TYPES = {'character varying', 'character'}
FLOAT_TYPES = {'double precision', 'real'}
TIMESTAMP_TYPES = {'timestamp without time zone', 'timestamp with time zone'}


def big_frame(n: int = 1000) -> pd.DataFrame:
    """At most ~1000 rows: INSERTs cost ~130ms/row through a SQL proxy."""
    ids = range(1, n + 1)
    return pd.DataFrame({
        'id': list(ids),
        'name': [f"name {i}, \"q\" o'x" if i % 7 == 0 else f"name{i}" for i in ids],
        'score': [i / 3 if i % 5 else None for i in ids],
        'active': [i % 2 == 0 for i in ids],
        'updated_at': pd.date_range('2024-01-01', periods=n, freq='h'),
    })


def assert_same_data(got: pd.DataFrame, expected: pd.DataFrame) -> None:
    assert list(got.columns) == list(expected.columns), \
        f"columns differ: {list(got.columns)} != {list(expected.columns)}"
    assert len(got) == len(expected), f"row count {len(got)} != {len(expected)}"
    got = got.sort_values('id').reset_index(drop=True)
    expected = expected.sort_values('id').reset_index(drop=True)
    pd.testing.assert_frame_equal(got, expected, check_dtype=False)


def assert_small_types(types: dict) -> None:
    assert types['id'] in INT_TYPES, types
    assert types['name'] in STRING_TYPES, types
    assert types['score'] in FLOAT_TYPES, types
    assert types['active'] == 'boolean', types
    assert types['updated_at'] in TIMESTAMP_TYPES, types
