"""Engines, temporary AWS credentials, per-run naming and cleanup for the live suite.

Everything that touches boto3, certifi or a database imports lazily, so
collecting this package never needs (or contacts) any of them.
"""
import importlib
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine

from .config import LiveConfig
from .guard import SqlGuard, TABLE_PREFIX
from .redaction import redact, register_secrets

TEMP_CREDS_SECONDS = 900  # the minimum GetSessionToken allows


# -- engines ------------------------------------------------------------
class TrackedEngine:
    """A guarded engine that remembers every raw DBAPI connection it opened.

    records-mover's DB drivers keep long-lived connections that hold
    AccessShareLocks after SELECTs; those would block our DROP TABLE.
    ``close()`` force-closes them all before disposing the engine.
    """

    def __init__(self, cfg: LiveConfig, guard: SqlGuard) -> None:
        import certifi
        from sqlalchemy import event
        self.guard = guard
        connect_args: Dict[str, Any] = {}
        if cfg.sslmode != 'disable':
            connect_args = {'sslmode': cfg.sslmode,
                            'sslrootcert': cfg.sslrootcert or certifi.where()}
        # A per-engine pool logger lets close() mute only this engine's pool
        self._pool_logger_name = f'live_{id(self):x}'
        self.engine: Engine = sa.create_engine(cfg.db_url, connect_args=connect_args,
                                               hide_parameters=True,
                                               pool_logging_name=self._pool_logger_name)
        self._dbapi_conns: List[Any] = []
        timeout_ms = cfg.statement_timeout_s * 1000

        @event.listens_for(self.engine, 'connect')
        def on_connect(dbapi_conn: Any, record: Any) -> None:
            cur = dbapi_conn.cursor()
            try:
                cur.execute(f"SET statement_timeout TO {int(timeout_ms)}")
            finally:
                cur.close()
            dbapi_conn.commit()
            self._dbapi_conns.append(dbapi_conn)

        guard.attach(self.engine)

    def close(self) -> None:
        # records-mover drivers may return connections to the pool after we force-close
        # them below; the pool's rollback on those would log 'connection already closed'
        pool_logger = f'sqlalchemy.pool.impl.QueuePool.{self._pool_logger_name}'
        logging.getLogger(pool_logger).setLevel(logging.CRITICAL)
        for conn in self._dbapi_conns:
            try:
                conn.close()
            except Exception:  # noqa: BLE001 - best effort
                pass
        self._dbapi_conns = []
        self.engine.dispose()


# -- temporary AWS credentials ------------------------------------------
def mint_boto3_session(profile: str) -> Any:
    """A boto3 session using 15-minute temporary credentials.

    Long-lived keys from the profile are used only to call STS; they are never
    handed to records-mover.
    """
    boto3: Any = importlib.import_module('boto3')  # the repo's stubs cover only part of boto3
    base = boto3.Session(profile_name=profile)
    creds = base.client('sts').get_session_token(DurationSeconds=TEMP_CREDS_SECONDS)['Credentials']
    register_secrets([creds['AccessKeyId'], creds['SecretAccessKey'], creds['SessionToken']])
    return boto3.Session(aws_access_key_id=creds['AccessKeyId'],
                         aws_secret_access_key=creds['SecretAccessKey'],
                         aws_session_token=creds['SessionToken'],
                         region_name=base.region_name or 'us-east-1')


def split_s3_url(url: str) -> Tuple[str, str]:
    parsed = urlparse(url)
    return parsed.netloc, parsed.path.lstrip('/')


def list_s3_keys(boto_session: Any, url: str) -> List[str]:
    bucket, prefix = split_s3_url(url)
    s3 = boto_session.client('s3')
    keys: List[str] = []
    token: Optional[str] = None
    while True:
        kwargs: Dict[str, Any] = {'Bucket': bucket, 'Prefix': prefix}
        if token:
            kwargs['ContinuationToken'] = token
        resp = s3.list_objects_v2(**kwargs)
        keys += [o['Key'] for o in resp.get('Contents', [])]
        if not resp.get('IsTruncated'):
            return keys
        token = resp['NextContinuationToken']


# -- database helpers ---------------------------------------------------
class Db:
    """Read-side helpers on a separate guarded engine (no long-lived connections)."""

    def __init__(self, cfg: LiveConfig, engine: Engine) -> None:
        self.schema = cfg.schema
        self.engine = engine

    def read(self, table: str) -> pd.DataFrame:
        with self.engine.connect() as conn:
            df = pd.read_sql(sa.text(f'select * from {self.schema}.{table} order by id'), conn)
        return df.reset_index(drop=True)

    def column_types(self, table: str) -> Dict[str, str]:
        with self.engine.connect() as conn:
            rows = conn.execute(sa.text(
                "select column_name, data_type from information_schema.columns "
                "where table_schema = :s and table_name = :t order by ordinal_position"),
                {'s': self.schema, 't': table}).fetchall()
        return {r[0]: r[1] for r in rows}


# -- per-run naming and cleanup -----------------------------------------
@dataclass
class RunContext:
    cfg: LiveConfig
    suffix: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    tables: List[str] = field(default_factory=list)

    def table(self, label: str) -> str:
        """A fresh, unique rm_test_ table name, registered for cleanup."""
        # The counter keeps names unique when tests in different files share a label
        name = f"{TABLE_PREFIX}{self.suffix}_{len(self.tables):02d}_{label}"
        self.tables.append(name)
        return name

    @property
    def s3_run_prefix(self) -> Optional[str]:
        return f"{self.cfg.s3_prefix}{self.suffix}/" if self.cfg.s3_prefix else None

    def cleanup(self) -> List[str]:
        """Drop every table and delete every S3 object made by this run.

        Returns a list of problems (empty when everything is gone).
        """
        problems: List[str] = []
        tracked = TrackedEngine(self.cfg, SqlGuard(self.cfg.schema, None))
        try:
            for t in self.tables:
                try:
                    with tracked.engine.begin() as conn:
                        conn.execute(sa.text(f'drop table if exists {self.cfg.schema}.{t}'))
                except Exception as e:  # noqa: BLE001
                    problems.append(f"drop {t}: {type(e).__name__}: {redact(str(e))[:200]}")
        finally:
            tracked.close()
        if self.s3_run_prefix:
            problems += self._cleanup_s3(self.s3_run_prefix)
        return problems

    def _cleanup_s3(self, run_prefix: str) -> List[str]:
        try:
            session = mint_boto3_session(self.cfg.aws_profile)
            bucket, key_prefix = split_s3_url(run_prefix)
            keys = [k for k in list_s3_keys(session, run_prefix) if k.startswith(key_prefix)]
            s3 = session.client('s3')
            for i in range(0, len(keys), 1000):
                s3.delete_objects(Bucket=bucket,
                                  Delete={'Objects': [{'Key': k} for k in keys[i:i + 1000]]})
            left = list_s3_keys(session, run_prefix)
            return [f"{len(left)} S3 objects left under {run_prefix}"] if left else []
        except Exception as e:  # noqa: BLE001
            return [f"S3 cleanup under {run_prefix}: {type(e).__name__}: {redact(str(e))[:200]}"]


# -- what a test gets ---------------------------------------------------
@dataclass
class LiveEnv:
    records: Any  # records_mover.records.Records
    engine: Engine  # the engine handed to records-mover (guarded)
    guard: SqlGuard
    db: Db
    run: RunContext
    schema: str
    boto_session: Any = None
    scratch_url: Optional[str] = None

    def s3_url(self, sub: str) -> str:
        assert self.scratch_url, 'no S3 configured for this test'
        return f"{self.scratch_url}{sub}"


def make_session(scratch_s3_url: Optional[str], boto_session: Any) -> Any:
    """A records-mover Session that can only use what we hand it."""
    # importlib: mypy cannot resolve records_mover's lazily exposed Session
    Session = getattr(importlib.import_module('records_mover'), 'Session')
    return Session(session_type='env',
                   scratch_s3_url=scratch_s3_url,
                   scratch_gcs_url=None,
                   default_boto3_session=boto_session,
                   default_gcs_client=None,
                   default_gcp_creds=None,
                   default_aws_creds_name=None,
                   default_gcp_creds_name=None)
