"""Live scenarios on the S3 fast path (Redshift COPY / UNLOAD via S3).

Skipped unless RECORDS_MOVER_LIVE_S3_PREFIX is set.  AWS access is a fresh
15-minute temporary credential per test, and the guard confines every
statement to this run's unique sub-prefix.
"""
from typing import Any, List

import pandas as pd
import pytest
from records_mover.records import ProcessingInstructions

from .data import (INT_TYPES, STRING_TYPES, assert_same_data, assert_small_types,
                   big_frame)
from .harness import LiveEnv, list_s3_keys

pytestmark = pytest.mark.live

N = 1000
FRAME = big_frame(N)


def _target(env: LiveEnv, table: str) -> Any:
    return env.records.targets.table(schema_name=env.schema, table_name=table,
                                     db_engine=env.engine)


def _source(env: LiveEnv, table: str) -> Any:
    return env.records.sources.table(schema_name=env.schema, table_name=table,
                                     db_engine=env.engine)


def _seed(env: LiveEnv, table: str) -> None:
    env.records.move(env.records.sources.dataframe(df=FRAME), _target(env, table))
    env.guard.reset()


def _file_names(env: LiveEnv, url: str) -> List[str]:
    return sorted(k.rsplit('/', 1)[-1] for k in list_s3_keys(env.boto_session, url))


def _assert_s3_used(env: LiveEnv, *kinds: str) -> None:
    for kind in kinds:
        assert kind in env.guard.kinds, f"{kind} did not run: {env.guard.summary()}"
    assert env.guard.s3_urls, 'no S3 URL appeared in any statement'
    assert all(u.startswith(env.scratch_url or '') for u in env.guard.s3_urls)


def test_dataframe_to_table_via_copy(with_s3: LiveEnv) -> None:
    table = with_s3.run.table('df_load')
    result = with_s3.records.move(with_s3.records.sources.dataframe(df=FRAME),
                                  _target(with_s3, table))
    assert result.move_count in (N, None)  # COPY doesn't report counts yet (ROADMAP Phase 3)
    assert_same_data(with_s3.db.read(table), FRAME)
    assert_small_types(with_s3.db.column_types(table))
    _assert_s3_used(with_s3, 'COPY')


def test_table_to_s3_records_directory_then_copy(with_s3: LiveEnv) -> None:
    src_table = with_s3.run.table('unload_src')
    dst_table = with_s3.run.table('from_dir')
    _seed(with_s3, src_table)
    dir_url = with_s3.s3_url('unloaded/')

    unloaded = with_s3.records.move(_source(with_s3, src_table),
                                    with_s3.records.targets.directory_from_url(
                                        output_url=dir_url))
    assert unloaded.move_count == N
    files = _file_names(with_s3, dir_url)
    assert '_manifest' in files and '_schema.json' in files, files
    _assert_s3_used(with_s3, 'UNLOAD')

    with_s3.guard.reset()
    reloaded = with_s3.records.move(with_s3.records.sources.directory_from_url(url=dir_url),
                                    _target(with_s3, dst_table))
    assert reloaded.move_count in (N, None)  # COPY doesn't report counts yet (ROADMAP Phase 3)
    assert_same_data(with_s3.db.read(dst_table), FRAME)
    assert with_s3.db.column_types(dst_table) == with_s3.db.column_types(src_table)
    _assert_s3_used(with_s3, 'COPY')


def test_csv_to_table_via_copy(with_s3: LiveEnv, tmp_path: Any) -> None:
    table = with_s3.run.table('csv_load')
    path = tmp_path / 'in.csv'
    path.write_text('id,city,population,founded\n1,boston,650000,1630-09-07\n'
                    '2,denver,715000,1858-11-22\n3,"new york, ny",8300000,1624-01-01\n')
    result = with_s3.records.move(with_s3.records.sources.data_url(input_url=f'file://{path}'),
                                  _target(with_s3, table))
    assert result.move_count in (3, None)  # COPY doesn't report counts yet (ROADMAP Phase 3)
    got = with_s3.db.read(table)
    assert got['city'].tolist() == ['boston', 'denver', 'new york, ny']
    assert got['population'].tolist() == [650000, 715000, 8300000]
    assert [str(v)[:10] for v in got['founded']] == ['1630-09-07', '1858-11-22', '1624-01-01']
    types = with_s3.db.column_types(table)
    assert types['id'] in INT_TYPES and types['population'] in INT_TYPES, types
    assert types['city'] in STRING_TYPES, types
    _assert_s3_used(with_s3, 'COPY')


def test_table_to_table_via_unload_and_copy(with_s3: LiveEnv) -> None:
    src_table = with_s3.run.table('t2t_src')
    dst_table = with_s3.run.table('t2t_dst')
    _seed(with_s3, src_table)
    result = with_s3.records.move(_source(with_s3, src_table), _target(with_s3, dst_table))
    assert result.move_count in (N, None)  # COPY doesn't report counts yet (ROADMAP Phase 3)
    assert_same_data(with_s3.db.read(dst_table), FRAME)
    assert with_s3.db.column_types(dst_table) == with_s3.db.column_types(src_table)
    _assert_s3_used(with_s3, 'UNLOAD', 'COPY')


def test_table_to_dataframe_via_unload(with_s3: LiveEnv) -> None:
    table = with_s3.run.table('to_df')
    _seed(with_s3, table)
    with _source(with_s3, table).to_dataframes_source(ProcessingInstructions()) as dfs:
        df = pd.concat(list(dfs.dfs))
    assert_same_data(df, FRAME)
    assert with_s3.guard.summary(), 'expected the reads to go through the guard'
