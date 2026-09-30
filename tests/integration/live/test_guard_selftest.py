"""Always-on, pure self-tests for the guard, redaction and config (no network, no DB)."""
import logging
from typing import Any, Dict

import pytest

from .config import LiveConfigError, is_enabled, load_config, resolve_dbt_value
from .guard import GuardViolation, SqlGuard
from .redaction import install_log_redaction, redact, register_secrets

PREFIX = 's3://bucket/records-mover-test/abc123/'


@pytest.fixture
def guard() -> SqlGuard:
    return SqlGuard('myschema', PREFIX)


@pytest.mark.parametrize('sql', [
    'select * from other.anything',
    'select 1',
    'SET statement_timeout TO 1000',
    'CREATE TABLE myschema.rm_test_abc_t (id bigint)',
    'create table if not exists "MySchema"."rm_test_abc_t" (id bigint)',
    'DROP TABLE IF EXISTS myschema.rm_test_abc_t',
    'INSERT INTO myschema.rm_test_abc_t (id) VALUES (%(id)s)',
    'DELETE FROM myschema.rm_test_abc_t',
    'ALTER TABLE myschema.rm_test_abc_t ADD COLUMN x int',
])
def test_in_scope_statements_allowed(guard: SqlGuard, sql: str) -> None:
    guard.check(sql, {'id': 1})


@pytest.mark.parametrize('sql', [
    'DROP TABLE myschema.real_table',
    'DROP TABLE otherschema.rm_test_abc_t',
    'DROP TABLE rm_test_abc_t',
    'TRUNCATE myschema.customers',
    'DELETE FROM otherschema.rm_test_x',
    'INSERT INTO myschema.customers VALUES (1)',
    'UPDATE myschema.customers SET x = 1',
    'GRANT ALL ON myschema.customers TO PUBLIC',
    'CREATE TABLE myschema.rm_test_abc_t AS select * from otherschema.secret',
    'DROP TABLE myschema.rm_test_a, myschema.customers',
    'ALTER TABLE myschema.customers ADD COLUMN x int',
])
def test_out_of_scope_writes_rejected(guard: SqlGuard, sql: str) -> None:
    with pytest.raises(GuardViolation):
        guard.check(sql, None)


def test_unload_with_bound_params_in_scope_allowed(guard: SqlGuard) -> None:
    guard.check('UNLOAD (%(select)s) TO %(url)s CREDENTIALS %(creds)s MANIFEST',
                {'select': 'select * from myschema.rm_test_abc_t',
                 'url': PREFIX + 'unloaded/data_',
                 'creds': 'aws_access_key_id=AKIAEXAMPLE;aws_secret_access_key=abc'})
    assert guard.kinds == ['UNLOAD']
    assert guard.s3_urls == [PREFIX + 'unloaded/data_']


def test_unload_of_out_of_scope_table_rejected(guard: SqlGuard) -> None:
    with pytest.raises(GuardViolation):
        guard.check('UNLOAD (%s) TO %s', ('select * from myschema.customers',
                                          PREFIX + 'x/'))
    with pytest.raises(GuardViolation):
        guard.check('UNLOAD (%s) TO %s', ('select * from otherschema.rm_test_abc_t',
                                          PREFIX + 'x/'))


def test_unload_with_out_of_scope_join_rejected(guard: SqlGuard) -> None:
    with pytest.raises(GuardViolation):
        guard.check('UNLOAD (%(q)s) TO %(u)s',
                    {'q': 'select * from myschema.rm_test_abc_t a join myschema.customers b '
                          'on a.id = b.id', 'u': PREFIX})


def test_s3_url_outside_prefix_rejected_in_params_and_text(guard: SqlGuard) -> None:
    with pytest.raises(GuardViolation):
        guard.check('UNLOAD (%(q)s) TO %(u)s',
                    {'q': 'select * from myschema.rm_test_abc_t',
                     'u': 's3://bucket/records-mover-test/other-run/'})
    with pytest.raises(GuardViolation):
        guard.check("COPY myschema.rm_test_abc_t FROM 's3://other-bucket/x' ", None)


def test_copy_from_in_prefix_allowed(guard: SqlGuard) -> None:
    guard.check(f"COPY myschema.rm_test_abc_t FROM '{PREFIX}scratch/_manifest' MANIFEST", None)
    assert guard.kinds == ['COPY']


def test_any_s3_rejected_when_no_prefix() -> None:
    no_s3 = SqlGuard('myschema', None)
    with pytest.raises(GuardViolation):
        no_s3.check("COPY myschema.rm_test_abc_t FROM 's3://bucket/x'", None)


def test_iam_role_rejected(guard: SqlGuard) -> None:
    with pytest.raises(GuardViolation):
        guard.check(f"COPY myschema.rm_test_abc_t FROM '{PREFIX}m' "
                    "IAM_ROLE 'arn:aws:iam::1:role/x'", None)


def test_violation_messages_do_not_leak_secrets(guard: SqlGuard) -> None:
    register_secrets(['SUPERSECRETVALUE123'])
    with pytest.raises(GuardViolation) as excinfo:
        guard.check("COPY myschema.customers FROM 's3://b/x' "
                    "CREDENTIALS 'aws_secret_access_key=SUPERSECRETVALUE123'", None)
    assert 'SUPERSECRETVALUE123' not in str(excinfo.value)


def test_redact_registered_and_clause_shaped_secrets() -> None:
    register_secrets(['AKIAEXAMPLEKEY0001'])
    out = redact("key AKIAEXAMPLEKEY0001 and aws_secret_access_key=notregistered99 "
                 "token 'zzz'")
    assert 'AKIAEXAMPLEKEY0001' not in out
    assert 'notregistered99' not in out


def test_log_records_are_redacted(caplog: Any) -> None:
    register_secrets(['LOGSECRET-abcdef'])
    install_log_redaction()
    with caplog.at_level(logging.INFO):
        logging.getLogger('some.module').info('creds %s', 'LOGSECRET-abcdef')
    assert 'LOGSECRET-abcdef' not in caplog.text


def test_dbt_env_var_resolution() -> None:
    env = {'PW': 'hunter2'}
    assert resolve_dbt_value("{{ env_var('PW') }}", env) == 'hunter2'
    assert resolve_dbt_value('{{ env_var("NOPE", "dflt") }}', env) == 'dflt'
    assert resolve_dbt_value('plain', env) == 'plain'
    assert resolve_dbt_value(5439, env) == 5439


def _env(**overrides: str) -> Dict[str, str]:
    env = {'RECORDS_MOVER_LIVE_DB_URL': 'redshift+psycopg2://u:p@h:5439/db',
           'RECORDS_MOVER_LIVE_SCHEMA': 'scratch',
           'RECORDS_MOVER_LIVE_ALLOWED_SCHEMAS': 'scratch, other'}
    env.update(overrides)
    return env


def test_load_config_ok_and_normalizes_prefix() -> None:
    cfg = load_config(_env(RECORDS_MOVER_LIVE_S3_PREFIX='s3://b/p'))
    assert cfg.s3_prefix == 's3://b/p/'
    assert cfg.allowed_schemas == ('scratch', 'other')
    assert cfg.aws_profile == 'default' and cfg.sslmode == 'verify-full'
    assert 'redshift+psycopg2' not in repr(cfg)


def test_load_config_requires_schema_in_allowed_list() -> None:
    with pytest.raises(LiveConfigError):
        load_config(_env(RECORDS_MOVER_LIVE_SCHEMA='prod'))
    with pytest.raises(LiveConfigError):
        load_config({'RECORDS_MOVER_LIVE_DB_URL': 'x'})


def test_is_enabled_only_for_exactly_1() -> None:
    assert is_enabled({'RECORDS_MOVER_LIVE': '1'})
    assert not is_enabled({'RECORDS_MOVER_LIVE': 'true'})
    assert not is_enabled({})


def _hidden_write_guard() -> SqlGuard:
    return SqlGuard('myschema', s3_prefix='s3://bucket/records-mover-test')


@pytest.mark.parametrize('sql', [
    'SELECT * INTO public.stolen FROM myschema.rm_test_x',
    'WITH x AS (SELECT 1) INSERT INTO public.t SELECT * FROM x',
    'SELECT 1; DROP TABLE public.important',
    '/* hi */ -- x\n DROP TABLE public.important',
])
def test_hidden_out_of_scope_writes_blocked(sql: str) -> None:
    with pytest.raises(GuardViolation):
        _hidden_write_guard().check(sql)


@pytest.mark.parametrize('sql', [
    'SELECT * INTO myschema.rm_test_y FROM myschema.rm_test_x',
    'SELECT * FROM myschema.rm_test_x',
    'SELECT 1; SELECT 2',
])
def test_in_scope_or_read_only_allowed(sql: str) -> None:
    _hidden_write_guard().check(sql)


def test_prefix_without_slash_does_not_match_sibling() -> None:
    guard = _hidden_write_guard()
    with pytest.raises(GuardViolation):
        guard.check_s3_url('s3://bucket/records-mover-test-other/file.csv')
    guard.check_s3_url('s3://bucket/records-mover-test/run/file.csv')


def test_long_lived_access_key_blocked() -> None:
    guard = _hidden_write_guard()
    with pytest.raises(GuardViolation) as e:
        guard.check("COPY myschema.rm_test_x FROM 's3://bucket/records-mover-test/m' "
                    "ACCESS_KEY_ID 'AKIAABCDEFGHIJKLMNOP' SECRET_ACCESS_KEY 'x'")
    assert 'AKIA' not in str(e.value)
    guard.check("COPY myschema.rm_test_x FROM 's3://bucket/records-mover-test/m' "
                "ACCESS_KEY_ID 'ASIAABCDEFGHIJKLMNOP' SECRET_ACCESS_KEY 'x'")
