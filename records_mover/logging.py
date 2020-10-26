import os
import sys
import logging
import io
from typing import IO, Set, Optional


def _adjusted_log_level(default_log_level: int, name: str) -> int:
    log_level_str = os.environ.get(name.upper() + '_LOG_LEVEL', None)
    if log_level_str is None:
        return default_log_level
    return getattr(logging, log_level_str.upper())


_secrets: Set[str] = set()


def register_secret(secret: Optional[object]) -> None:
    if secret is not None:
        _secrets.add(str(secret))


# https://stackoverflow.com/questions/45469808/how-to-wrap-a-python-text-stream-to-replace-strings-on-the-fly
class SecretsRedactingLogStream(io.TextIOBase):
    def __init__(self, underlying: IO[str]) -> None:
        self.underlying = underlying
        self.secrets = ['records']

    def write(self, s: str) -> int:
        for secret in _secrets:
            if secret in s:
                replacement = '*' * len(secret)
                s = s.replace(secret, replacement)
        return self.underlying.write(s)


def set_stream_logging(name: str = 'records_mover',
                       level: int = logging.INFO,
                       stream: IO[str] = sys.stdout,
                       fmt: str = '%(asctime)s - %(message)s',
                       datefmt: str = '%H:%M:%S') -> None:
    """
    records-mover logs details about its operations using Python logging.  This method is a
    simple way to configure that logging to be output to a stream (by default, stdout).

    You can use it for other things (e.g., dependencies of
    records-mover) by adjusting the 'name' argument.

    :param name: Name of the package to set logging under.  If set to 'foo', you can set a log
      variable FOO_LOG_LEVEL to the log level threshold you'd like to set (INFO/WARNING/etc) - so
      you can by default set, say, export RECORDS_MOVER_LOG_LEVEL=WARNING to quiet down loging, or
      export RECORDS_MOVER_LOG_LEVEL=DEBUG to increase it.
    :param level: Logging more detailed than this will not be output to the stream.
    :param stream: Stream which logging should be sent (e.g., sys.stdout, sys.stdin, or perhaps a
      file you open)
    :param fmt: Logging format to send to Python'slogging.Formatter() - determines what details
      will be sent.
    :param datefmt: Date format to send to Python'slogging.Formatter() - determines how the current
      date/time will be recorded in the log.
    """
    adjusted_level = _adjusted_log_level(level, name)
    logger = logging.getLogger(name)
    logger.setLevel(adjusted_level)
    wrapper = SecretsRedactingLogStream(stream)
    handler = logging.StreamHandler(stream=wrapper)
    handler.setLevel(adjusted_level)
    formatter = logging.Formatter(fmt, datefmt)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
