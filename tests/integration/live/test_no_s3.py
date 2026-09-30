"""Live scenarios that need no AWS/GCP access at all (rows are INSERTed).

Keep the data tiny: INSERT through a SQL proxy costs about 130ms per row.
"""
from typing import Any

import pandas as pd
import pytest
from records_mover.records import ExistingTableHandling, ProcessingInstructions

from .data import (OBJECT_TYPES, SMALL, INT_TYPES, STRING_TYPES, assert_same_data,
                   assert_small_types)
from .harness import LiveEnv

pytestmark = pytest.mark.live


def _target(env: LiveEnv, table: str, **kwargs: Any) -> Any:
    return env.records.targets.table(schema_name=env.schema, table_name=table,
                                     db_engine=env.engine, **kwargs)


def _seed(env: LiveEnv, table: str, df: pd.DataFrame = SMALL) -> None:
    env.records.move(env.records.sources.dataframe(df=df), _target(env, table))


def _assert_no_s3_used(env: LiveEnv) -> None:
    assert not env.guard.s3_urls, 'no S3 URL should have been used'
    assert 'COPY' not in env.guard.kinds and 'UNLOAD' not in env.guard.kinds, env.guard.summary()


def test_dataframe_to_new_table(no_s3: LiveEnv) -> None:
    table = no_s3.run.table('df_load')
    result = no_s3.records.move(no_s3.records.sources.dataframe(df=SMALL), _target(no_s3, table))
    assert result.move_count == len(SMALL)
    assert_same_data(no_s3.db.read(table), SMALL)
    assert_small_types(no_s3.db.column_types(table))
    assert 'INSERT' in no_s3.guard.kinds, no_s3.guard.summary()
    _assert_no_s3_used(no_s3)


def test_table_to_dataframe(no_s3: LiveEnv) -> None:
    table = no_s3.run.table('to_df')
    _seed(no_s3, table)
    src = no_s3.records.sources.table(schema_name=no_s3.schema, table_name=table,
                                      db_engine=no_s3.engine)
    with src.to_dataframes_source(ProcessingInstructions()) as dfs:
        df = pd.concat(list(dfs.dfs))
    assert_same_data(df, SMALL)
    assert str(df['id'].dtype).lower().startswith('int'), df.dtypes  # int64 or nullable Int64
    assert df['active'].dtype == bool, df.dtypes
    assert str(df['updated_at'].dtype).startswith('datetime64'), df.dtypes
    _assert_no_s3_used(no_s3)


def test_table_to_table(no_s3: LiveEnv) -> None:
    src_table = no_s3.run.table('t2t_src')
    dst_table = no_s3.run.table('t2t_dst')
    _seed(no_s3, src_table)
    src = no_s3.records.sources.table(schema_name=no_s3.schema, table_name=src_table,
                                      db_engine=no_s3.engine)
    result = no_s3.records.move(src, _target(no_s3, dst_table))
    assert result.move_count == len(SMALL)
    assert_same_data(no_s3.db.read(dst_table), SMALL)
    assert no_s3.db.column_types(dst_table) == no_s3.db.column_types(src_table)
    _assert_no_s3_used(no_s3)


def test_local_csv_to_table(no_s3: LiveEnv, tmp_path: Any) -> None:
    table = no_s3.run.table('csv_load')
    path = tmp_path / 'in.csv'
    path.write_text('id,city,population\n1,boston,650000\n2,denver,715000\n'
                    '3,"new york, ny",8300000\n')
    src = no_s3.records.sources.data_url(input_url=f'file://{path}')
    result = no_s3.records.move(src, _target(no_s3, table))
    assert result.move_count == 3
    got = no_s3.db.read(table)
    assert got['city'].tolist() == ['boston', 'denver', 'new york, ny']
    assert got['population'].tolist() == [650000, 715000, 8300000]
    types = no_s3.db.column_types(table)
    assert types['id'] in INT_TYPES and types['population'] in INT_TYPES, types
    assert types['city'] in STRING_TYPES, types
    _assert_no_s3_used(no_s3)


def test_existing_table_delete_and_overwrite(no_s3: LiveEnv) -> None:
    table = no_s3.run.table('overwrite')
    _seed(no_s3, table)
    tgt = _target(no_s3, table,
                  existing_table_handling=ExistingTableHandling.DELETE_AND_OVERWRITE)
    no_s3.records.move(no_s3.records.sources.dataframe(df=SMALL.head(2)), tgt)
    assert_same_data(no_s3.db.read(table), SMALL.head(2))


def test_existing_table_append(no_s3: LiveEnv) -> None:
    table = no_s3.run.table('append')
    _seed(no_s3, table, SMALL.head(2))
    tgt = _target(no_s3, table, existing_table_handling=ExistingTableHandling.APPEND)
    no_s3.records.move(no_s3.records.sources.dataframe(df=SMALL.tail(1)), tgt)
    assert_same_data(no_s3.db.read(table), SMALL)


def test_existing_table_drop_and_recreate(no_s3: LiveEnv) -> None:
    table = no_s3.run.table('recreate')
    _seed(no_s3, table)
    tgt = _target(no_s3, table, existing_table_handling=ExistingTableHandling.DROP_AND_RECREATE)
    no_s3.records.move(no_s3.records.sources.dataframe(df=SMALL.head(1)), tgt)
    assert_same_data(no_s3.db.read(table), SMALL.head(1))
    assert 'DROP' in no_s3.guard.kinds, no_s3.guard.summary()


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason='known gap: DataFrame Decimal/date object columns land as varchar '
                          '(ROADMAP Phase 3 item 5); strict so this flips when fixed')
def test_dataframe_object_columns_keep_their_types(no_s3: LiveEnv) -> None:
    table = no_s3.run.table('object_types')
    no_s3.records.move(no_s3.records.sources.dataframe(df=OBJECT_TYPES),
                       _target(no_s3, table))
    types = no_s3.db.column_types(table)
    assert types['amount'] == 'numeric', types
    assert types['signup_date'] == 'date', types
