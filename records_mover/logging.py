import os
import sys
import logging
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


class SecretsRedactingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        print(f"VMB: record.msg: [[[{record.msg}]]]")
        # print(f"VMB: record.sinfo: [[[{record.sinfo}]]]")
        print(f"VMB: record.args: [[[{record.args}]]]")
        print(f"VMB: record.exc_info: [[[{record.exc_info}]]]")
        print(f"VMB: _secrets: [[[{_secrets}]]]")

        for secret in _secrets:
            if secret in str(record.msg):
                # Try redacting msg alone:
                replacement = '*' * len(secret)
                record.msg = str(record.msg).replace(secret, replacement)
        if record.exc_info is not None:
            type_, value_, traceback = record.exc_info
            print(f"VMB: type(record.exc_info[1]): {type(record.exc_info[1])}")
            # VMB: type(record.exc_info[1]): <class 'sqlalchemy.exc.ProgrammingError'>
            # VMB: type(record.exc_info[2]): <class 'traceback'>
            print(f"VMB: type(record.exc_info[2]): {type(record.exc_info[2])}")
            pass
            # for secret in _secrets:
            #     # Try redacting msg alone:
            #     replacement = '*' * len(secret)
            #     record.msg = record.msg.replace(secret, replacement)
            #     redacted = True

        return True  # yes, log this


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

    :param name: Name of the package to set logging under.  If set
    to 'foo', you can set a log variable FOO_LOG_LEVEL to the log
    level threshold you'd like to set (INFO/WARNING/etc) - so you
    can by default set, say, export
    RECORDS_MOVER_LOG_LEVEL=WARNING to quiet down loging, or
    export RECORDS_MOVER_LOG_LEVEL=DEBUG to increase it.
    :param level: Logging more detailed than this will not be output to the stream.
    :param stream: Stream which logging should be sent (e.g., sys.stdout, sys.stdin, or perhaps
    a file you open)
    :param fmt: Logging format to send to Python'slogging.Formatter() - determines what details
     will be sent.
    :param datefmt: Date format to send to Python'slogging.Formatter() - determines how the
    current date/time will be recorded in the log.
    """
    adjusted_level = _adjusted_log_level(level, name)
    logger = logging.getLogger(name)
    logger.setLevel(adjusted_level)
    logging_filter = SecretsRedactingFilter()
    handler = logging.StreamHandler(stream=stream)
    handler.addFilter(logging_filter)
    handler.setLevel(adjusted_level)
    formatter = logging.Formatter(fmt, datefmt)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
