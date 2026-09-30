"""Fixtures and opt-in gating for the live acceptance suite (see README.md)."""
import os
from typing import Any, Generator

import pytest

from .config import LiveConfig, LiveConfigError, is_enabled, load_config
from .harness import (Db, LiveEnv, RunContext, TrackedEngine, make_session,
                      mint_boto3_session)
from .guard import SqlGuard
from .redaction import install_log_redaction, redact

_HERE = os.path.dirname(os.path.abspath(__file__))
SKIP_REASON = ('live acceptance test: set RECORDS_MOVER_LIVE=1 '
               '(see tests/integration/live/README.md)')


def pytest_configure(config: Any) -> None:
    config.addinivalue_line('markers', 'live: opt-in test against real Redshift/S3 '
                                       '(needs RECORDS_MOVER_LIVE=1)')
    if is_enabled():
        install_log_redaction()


def pytest_collection_modifyitems(config: Any, items: Any) -> None:
    if is_enabled():
        return
    skip = pytest.mark.skip(reason=SKIP_REASON)
    for item in items:
        if str(item.fspath).startswith(_HERE) and item.get_closest_marker('live'):
            item.add_marker(skip)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: Any, call: Any) -> Generator[Any, Any, None]:
    """Scrub secrets from failure text (SQLAlchemy errors embed the SQL)."""
    outcome = yield
    report = outcome.get_result()
    if is_enabled() and str(item.fspath).startswith(_HERE):
        if report.failed:
            report.longrepr = redact(report.longreprtext)
        report.sections = [(name, redact(text)) for name, text in report.sections]


@pytest.fixture(scope='session')
def live_config() -> LiveConfig:
    try:
        return load_config()
    except LiveConfigError as e:
        pytest.fail(str(e), pytrace=False)


@pytest.fixture(scope='session', autouse=False)
def _env_isolation() -> Generator[None, None, None]:
    mp = pytest.MonkeyPatch()
    for name in list(os.environ):
        if name.startswith(('GOOGLE_', 'SCRATCH_')):
            mp.delenv(name)
    mp.setenv('RECORDS_MOVER_SESSION_TYPE', 'env')
    yield
    mp.undo()


@pytest.fixture(scope='session')
def run(live_config: LiveConfig, _env_isolation: None) -> Generator[RunContext, None, None]:
    ctx = RunContext(live_config)
    yield ctx
    problems = ctx.cleanup()
    if problems:
        pytest.fail('CLEANUP INCOMPLETE - remove manually: ' + '; '.join(problems),
                    pytrace=False)


@pytest.fixture(scope='session')
def db(live_config: LiveConfig) -> Generator[Db, None, None]:
    tracked = TrackedEngine(live_config, SqlGuard(live_config.schema, None))
    yield Db(live_config, tracked.engine)
    tracked.close()


@pytest.fixture
def no_s3(live_config: LiveConfig, run: RunContext, db: Db) -> Generator[LiveEnv, None, None]:
    """records-mover with no AWS/GCP access at all; any s3:// statement is blocked."""
    tracked = TrackedEngine(live_config, SqlGuard(live_config.schema, None))
    session = make_session(scratch_s3_url=None, boto_session=None)
    yield LiveEnv(records=session.records, engine=tracked.engine, guard=tracked.guard,
                  db=db, run=run, schema=live_config.schema)
    tracked.close()  # release the locks records-mover's drivers hold


@pytest.fixture
def with_s3(live_config: LiveConfig, run: RunContext, db: Db) -> Generator[LiveEnv, None, None]:
    """records-mover with fresh 15-minute temporary AWS credentials, confined to the run prefix."""
    if not live_config.s3_prefix or not run.s3_run_prefix:
        pytest.skip('RECORDS_MOVER_LIVE_S3_PREFIX not set')
    boto_session = mint_boto3_session(live_config.aws_profile)
    tracked = TrackedEngine(live_config, SqlGuard(live_config.schema, run.s3_run_prefix))
    session = make_session(scratch_s3_url=run.s3_run_prefix, boto_session=boto_session)
    yield LiveEnv(records=session.records, engine=tracked.engine, guard=tracked.guard,
                  db=db, run=run, schema=live_config.schema, boto_session=boto_session,
                  scratch_url=run.s3_run_prefix)
    tracked.close()
