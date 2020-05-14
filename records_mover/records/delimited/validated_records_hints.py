from typing import NamedTuple, TypeVar
from .types import (RecordsHints, HintHeaderRow, HintFieldDelimiter,
                    HintCompression, HintRecordTerminator,
                    HintQuoting, HintQuoteChar, HintDoublequote,
                    HintEscape, HintEncoding, HintDateFormat,
                    HintTimeOnlyFormat, HintDateTimeFormatTz,
                    HintDateTimeFormat)
from .hints import Hints
from .hint import Hint


class ValidatedRecordsHints(NamedTuple):
    header_row: HintHeaderRow
    field_delimiter: HintFieldDelimiter
    compression: HintCompression
    record_terminator: HintRecordTerminator
    quoting: HintQuoting
    quotechar: HintQuoteChar
    doublequote: HintDoublequote
    escape: HintEscape
    encoding: HintEncoding
    dateformat: HintDateFormat
    timeonlyformat: HintTimeOnlyFormat
    datetimeformattz: HintDateTimeFormatTz
    datetimeformat: HintDateTimeFormat

    @staticmethod
    def validate(hints: RecordsHints,
                 fail_if_cant_handle_hint: bool) -> 'ValidatedRecordsHints':
        T = TypeVar('T')

        def v(hint: Hint[T]) -> T:
            return hint.validate(hints, fail_if_cant_handle_hint)

        return ValidatedRecordsHints(
            header_row=v(Hints.header_row),
            field_delimiter=v(Hints.field_delimiter),
            compression=v(Hints.compression),
            record_terminator=v(Hints.record_terminator),
            quoting=v(Hints.quoting),
            quotechar=v(Hints.quotechar),
            doublequote=v(Hints.doublequote),
            escape=v(Hints.escape),
            encoding=v(Hints.encoding),
            dateformat=v(Hints.dateformat),
            timeonlyformat=v(Hints.timeonlyformat),
            datetimeformattz=v(Hints.datetimeformattz),
            datetimeformat=v(Hints.datetimeformat),
        )
