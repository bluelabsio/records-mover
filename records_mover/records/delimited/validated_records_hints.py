from typing import NamedTuple, TypeVar
from .types import (UntypedRecordsHints, HintHeaderRow, HintFieldDelimiter,
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
    def validate(hints: UntypedRecordsHints,
                 fail_if_cant_handle_hint: bool) -> 'ValidatedRecordsHints':
        T = TypeVar('T')

        def v(hint: Hint[T]) -> T:
            return hint.validate(hints, fail_if_cant_handle_hint)

        return ValidatedRecordsHints(
            header_row=v(Hints.header_row.value),
            field_delimiter=v(Hints.field_delimiter.value),
            compression=v(Hints.compression.value),
            record_terminator=v(Hints.record_terminator.value),
            quoting=v(Hints.quoting.value),
            quotechar=v(Hints.quotechar.value),
            doublequote=v(Hints.doublequote.value),
            escape=v(Hints.escape.value),
            encoding=v(Hints.encoding.value),
            dateformat=v(Hints.dateformat.value),
            timeonlyformat=v(Hints.timeonlyformat.value),
            datetimeformattz=v(Hints.datetimeformattz.value),
            datetimeformat=v(Hints.datetimeformat.value),
        )
