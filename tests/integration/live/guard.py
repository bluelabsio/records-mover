"""Scope guard for the live acceptance suite.

Every statement sent through a guarded SQLAlchemy engine is checked *before*
it reaches Redshift.  Bound parameters are inspected as well as the statement
text, because sqlalchemy-redshift passes UNLOAD's inner SELECT and the S3
destination as bound parameters.

A statement is rejected if:

* it mentions an ``s3://`` URL outside the configured prefix (or any
  ``s3://`` URL when no prefix is configured);
* it mentions ``IAM_ROLE`` (not expected: the suite uses temporary keys);
* it is a write (CREATE/DROP/INSERT/DELETE/TRUNCATE/ALTER/GRANT/REVOKE/UPDATE/
  COPY/UNLOAD, plus VACUUM/ANALYZE/COMMENT) that does not reference
  ``<allowed_schema>.rm_test_<something>``, or that references any other
  schema-qualified table.
"""
import re
from typing import Any, Dict, Iterator, List, Optional

from sqlalchemy import event
from sqlalchemy.engine import Engine

from .redaction import redact

TABLE_PREFIX = 'rm_test_'

WRITE_KINDS = frozenset({'CREATE', 'DROP', 'INSERT', 'DELETE', 'TRUNCATE', 'ALTER', 'GRANT',
                         'REVOKE', 'UPDATE', 'COPY', 'UNLOAD', 'VACUUM', 'ANALYZE', 'COMMENT'})
# Statements whose bound parameters can carry SQL (UNLOAD's inner select)
_PARAMS_ARE_SQL = frozenset({'UNLOAD', 'COPY'})

_S3_URL = re.compile(r"s3://[^\s'\"]+", re.I)
_IAM_ROLE = re.compile(r'\bIAM_ROLE\b', re.I)
# Long-lived IAM user access key ids (temporary STS keys start with ASIA)
_LONG_LIVED_KEY = re.compile(r'\bAKIA[A-Z0-9]{16}\b')
_IDENT = r'(?:"[^"]+"|\w+)'
_QUALIFIED = rf'{_IDENT}\s*\.\s*{_IDENT}'
_REF_LIST = re.compile(rf'\b(?:from|join|into|table|update|exists|copy|on)\s+'
                       rf'({_QUALIFIED}(?:\s*,\s*{_QUALIFIED})*)', re.I)
_ANY_QUALIFIED = re.compile(_QUALIFIED)
_LEADING_NOISE = re.compile(r'^(?:\s+|--[^\n]*(?:\n|$)|/\*.*?\*/|\()+', re.S)
# Statements that don't start with a write keyword but still write
_HIDDEN_WRITE = re.compile(r'\b(?:INTO|INSERT|DELETE|UPDATE|CREATE|DROP|TRUNCATE|ALTER|'
                           r'GRANT|REVOKE|COPY|UNLOAD)\b', re.I)


class GuardViolation(RuntimeError):
    """Raised (before execution) when a statement is outside the agreed scope."""


def _flatten(parameters: Any) -> Iterator[str]:
    if parameters is None:
        return
    if isinstance(parameters, dict):
        for v in parameters.values():
            yield from _flatten(v)
    elif isinstance(parameters, (list, tuple, set, frozenset)):
        for v in parameters:
            yield from _flatten(v)
    else:
        yield str(parameters)


def statement_kind(statement: str) -> str:
    stripped = _LEADING_NOISE.sub('', statement)
    return stripped.split(None, 1)[0].upper() if stripped else ''


def _statement_parts(statement: str) -> List[str]:
    """Split on ';' so a write hidden behind a harmless first statement is still checked."""
    parts = [p for p in statement.split(';') if _LEADING_NOISE.sub('', p).strip()]
    return parts or [statement]


class SqlGuard:
    def __init__(self, allowed_schema: str, s3_prefix: Optional[str] = None,
                 table_prefix: str = TABLE_PREFIX) -> None:
        self.allowed_schema = allowed_schema.lower()
        if s3_prefix is not None and not s3_prefix.endswith('/'):
            s3_prefix += '/'  # so s3://b/prefix doesn't also match s3://b/prefix-other/
        self.s3_prefix = s3_prefix
        self.table_prefix = table_prefix.lower()
        #: statement kinds (first keyword) seen, in order, since the last reset()
        self.kinds: List[str] = []
        #: s3:// URLs seen in statements or bound parameters since the last reset()
        self.s3_urls: List[str] = []

    def reset(self) -> None:
        self.kinds = []
        self.s3_urls = []

    # -- helpers ---------------------------------------------------------
    def _in_scope(self, qualified: str) -> bool:
        parts = [p.strip().strip('"').lower() for p in qualified.split('.')]
        return (len(parts) == 2 and parts[0] == self.allowed_schema
                and parts[1].startswith(self.table_prefix))

    def check_s3_url(self, url: str) -> None:
        if self.s3_prefix is None or not url.startswith(self.s3_prefix):
            raise GuardViolation(f"S3 URL outside the test prefix blocked: {redact(url[:100])}")

    def _fail(self, what: str, statement: str) -> GuardViolation:
        return GuardViolation(f"{what}: {redact(' '.join(statement.split())[:160])}")

    # -- the check -------------------------------------------------------
    def check(self, statement: str, parameters: Any = None) -> None:
        parts = _statement_parts(statement)
        if len(parts) > 1:
            for part in parts:
                self.check(part, parameters)
            return
        kind = statement_kind(statement)
        params_text = ' '.join(_flatten(parameters))
        everything = f"{statement} {params_text}"

        urls = _S3_URL.findall(everything)
        for url in urls:
            self.check_s3_url(url)
        if _LONG_LIVED_KEY.search(everything):
            raise self._fail('long-lived AWS access key in SQL blocked', 'statement redacted')
        if _IAM_ROLE.search(everything):
            raise self._fail('IAM_ROLE not expected in the live suite', statement)

        is_write = kind in WRITE_KINDS or (kind in ('SELECT', 'WITH')
                                           and bool(_HIDDEN_WRITE.search(statement)))
        if is_write:
            scoped = f"{statement} {params_text}" if kind in _PARAMS_ARE_SQL else statement
            refs = [r for lst in _REF_LIST.findall(scoped) for r in _ANY_QUALIFIED.findall(lst)]
            if not any(self._in_scope(r) for r in _ANY_QUALIFIED.findall(scoped)):
                raise self._fail(f"out-of-scope {kind} blocked (needs "
                                 f"{self.allowed_schema}.{self.table_prefix}*)", statement)
            for ref in refs:
                if not self._in_scope(ref):
                    raise self._fail(f"{kind} referencing out-of-scope table {ref!r} blocked",
                                     statement)
        self.kinds.append(kind)
        self.s3_urls.extend(urls)

    # -- SQLAlchemy wiring -----------------------------------------------
    def attach(self, engine: Engine) -> None:
        def listener(conn: Any, cursor: Any, statement: str, parameters: Any,
                     context: Any, executemany: bool) -> None:
            self.check(statement, parameters)

        event.listen(engine, 'before_cursor_execute', listener)

    def summary(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for k in self.kinds:
            out[k] = out.get(k, 0) + 1
        return out
