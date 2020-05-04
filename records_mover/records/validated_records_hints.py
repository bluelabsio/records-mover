from typing import NamedTuple, Union, Literal, Optional, TypeVar, List
from .types import (RecordsHints, HintHeaderRow, HintFieldDelimiter,
                    HintCompression, HintRecordTerminator,
                    HintQuoting, HintQuoteChar, HintDoublequote,
                    HintEscape, HintEncoding, HintDateFormat,
                    HintTimeOnlyFormat, HintDateTimeFormatTz,
                    HintDateTimeFormat)
from .types import (VALID_COMPRESSIONS, VALID_QUOTING, VALID_ESCAPE,
                    VALID_ENCODING, VALID_DATEFORMATS, VALID_TIMEONLYFORMATS,
                    VALID_DATETIMEFORMATTZS, VALID_DATETIMEFORMATS)
from .hints import cant_handle_hint


# TODO: Read up on namedtuple vs dataclass
# TODO: Read up on autovalidating libraries
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
        def validate_boolean(hint_name: str) -> Union[Literal[True], Literal[False]]:
            x = hints[hint_name]
            if x is True:
                return True
            if x is False:
                return False
            cant_handle_hint(fail_if_cant_handle_hint=fail_if_cant_handle_hint,
                             hint_name=hint_name,
                             hints=hints)
            return True

        def validate_string(hint_name: str) -> str:
            x = hints[hint_name]
            if isinstance(x, str):
                return x
            cant_handle_hint(fail_if_cant_handle_hint=fail_if_cant_handle_hint,
                             hint_name=hint_name,
                             hints=hints)
            return str(x)

        def validate_optional_string(hint_name: str) -> Optional[str]:
            x = hints[hint_name]
            if x is None:
                return x
            if isinstance(x, str):
                return x
            cant_handle_hint(fail_if_cant_handle_hint=fail_if_cant_handle_hint,
                             hint_name=hint_name,
                             hints=hints)
            return str(x)

        A = TypeVar('A')

        def validate_literal(valid_list: List[A],
                             hint_name: str,
                             default: A) -> A:
            x = hints[hint_name]
            # MyPy doesn't like looking for a generic optional string
            # in a list of specific optional strings.  It's wrong;
            # that's perfectly safe
            try:
                i = valid_list.index(x)  # type: ignore
                return valid_list[i]
            except ValueError:
                cant_handle_hint(fail_if_cant_handle_hint=fail_if_cant_handle_hint,
                                 hint_name=hint_name,
                                 hints=hints)
                return default

        def validate_compression() -> HintCompression:
            return validate_literal(VALID_COMPRESSIONS,
                                    'compression',
                                    default=None)

        def validate_quoting() -> HintQuoting:
            return validate_literal(VALID_QUOTING,
                                    'quoting',
                                    default='minimal')

        def validate_escape() -> HintEscape:
            return validate_literal(VALID_ESCAPE,
                                    'escape',
                                    default='\\')

        def validate_encoding() -> HintEncoding:
            return validate_literal(VALID_ENCODING,
                                    'encoding',
                                    default='UTF8')

        def validate_dateformat() -> HintDateFormat:
            return validate_literal(VALID_DATEFORMATS,
                                    'dateformat',
                                    default='YYYY-MM-DD')

        def validate_timeonlyformat() -> HintTimeOnlyFormat:
            return validate_literal(VALID_TIMEONLYFORMATS,
                                    'timeonlyformat',
                                    default="HH24:MI:SS")

        def validate_datetimeformattz() -> HintDateTimeFormatTz:
            return validate_literal(VALID_DATETIMEFORMATTZS,
                                    'datetimeformattz',
                                    default="YYYY-MM-DD HH24:MI:SSOF")

        def validate_datetimeformat() -> HintDateTimeFormat:
            return validate_literal(VALID_DATETIMEFORMATS,
                                    'datetimeformat',
                                    default="YYYY-MM-DD HH24:MI:SS")

        header_row = validate_boolean('header-row')
        field_delimiter = validate_string('field-delimiter')
        compression = validate_compression()
        record_terminator = validate_string('record-terminator')
        quoting = validate_quoting()
        quotechar = validate_string('quotechar')
        doublequote = validate_boolean('doublequote')
        escape = validate_escape()
        encoding = validate_encoding()
        dateformat = validate_dateformat()
        timeonlyformat = validate_timeonlyformat()
        datetimeformattz = validate_datetimeformattz()
        datetimeformat = validate_datetimeformat()
        # TODO: After one create functions
        return ValidatedRecordsHints(
            header_row=header_row,
            field_delimiter=field_delimiter,
            compression=compression,
            record_terminator=record_terminator,
            quoting=quoting,
            quotechar=quotechar,
            doublequote=doublequote,
            escape=escape,
            encoding=encoding,
            dateformat=dateformat,
            timeonlyformat=timeonlyformat,
            datetimeformattz=datetimeformattz,
            datetimeformat=datetimeformat,
        )
