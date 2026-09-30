"""Keep credentials out of test output and logs.

Secrets minted or read at runtime (AWS temporary credentials, the database
password) are registered here, and everything that could reach a terminal or
log file - log records, pytest failure reports, guard error messages - is
passed through :func:`redact`.
"""
import logging
import re
from typing import Any, Iterable, Set

REDACTED = '***REDACTED***'

_secrets: Set[str] = set()

# Shapes of credential clauses in COPY/UNLOAD SQL, as a belt-and-braces
# measure for secrets that were never registered.
_CLAUSE = re.compile(r"(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN|aws_access_key_id|"
                     r"aws_secret_access_key|token)(\s*[=']\s*)'?[^'\s;]+", re.I)


def register_secrets(values: Iterable[str]) -> None:
    for v in values:
        # Very short values would mangle unrelated text when substituted
        if v and len(v) >= 4:
            _secrets.add(v)


def redact(text: str) -> str:
    # Longest first so a secret that contains another is fully masked
    for secret in sorted(_secrets, key=len, reverse=True):
        text = text.replace(secret, REDACTED)
    return _CLAUSE.sub(lambda m: f"{m.group(1)}{m.group(2)}***", text)


def install_log_redaction() -> None:
    """Redact every log record at creation time, whichever logger emits it."""
    previous = logging.getLogRecordFactory()
    if getattr(previous, '_rm_live_redacting', False):
        return

    def factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        record = previous(*args, **kwargs)
        record.msg = redact(record.getMessage())
        record.args = ()
        return record

    factory._rm_live_redacting = True  # type: ignore[attr-defined]
    logging.setLogRecordFactory(factory)
