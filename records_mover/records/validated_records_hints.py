from typing_inspect import is_literal_type, get_args
from typing import NamedTuple, Union, Optional, TypeVar, List, Type, Collection, Generic
from typing_extensions import Literal
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

HintT = TypeVar('HintT')


class HintValidator:
    def __init__(self,
                 fail_if_cant_handle_hint: bool,
                 hints: RecordsHints):
        self.fail_if_cant_handle_hint = fail_if_cant_handle_hint
        self.hints = hints

        class Hint(Generic[HintT]):
            def __init__(self,
                         type_: Type[HintT],
                         hint_name: str,  # TODO: HintName,
                         default: HintT):
                self.default = default
                self.type_ = type_
                self.hint_name = hint_name
                self.valid_values: List[HintT] = list(get_args(type_))

            def validate_literal(self) -> HintT:
                # MyPy doesn't like looking for a generic optional string
                # in a list of specific optional strings.  It's wrong;
                # that's perfectly safe
                x: object = hints[self.hint_name]
                try:
                    i = self.valid_values.index(x)  # type: ignore
                    return self.valid_values[i]
                except ValueError:
                    cant_handle_hint(fail_if_cant_handle_hint=fail_if_cant_handle_hint,
                                     hint_name=self.hint_name,
                                     hints=hints)
                    return self.default

            def validate(self) -> HintT:
                assert is_literal_type(self.type_)

                return self.validate_literal()

        self.hintdatetimeformattz = Hint(HintDateTimeFormatTz,  # type: ignore
                                         "datetimeformattz",
                                         "YYYY-MM-DD HH24:MI:SSOF")


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
        validator = HintValidator(hints=hints,
                                  fail_if_cant_handle_hint=fail_if_cant_handle_hint)

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

        T = TypeVar('T')

        def validate_literal_type(type_: Type[T],
                                  default: T,
                                  hint_name: str) -> T:
            assert is_literal_type(type_)
            valid_values: List[T] = list(get_args(type_))
            return validate_literal(valid_values,
                                    hint_name,
                                    default=default)

        def validate_literal_type_from_class(type_: Type[T],
                                             default: T,
                                             hint_name: str) -> T:
            assert is_literal_type(type_)
            valid_values: List[T] = list(get_args(type_))
            return validate_literal(valid_values,
                                    hint_name,
                                    default=default)

        def validate_datetimeformattz() -> HintDateTimeFormatTz:
            return validate_literal_type(HintDateTimeFormatTz,  # type: ignore
                                         default="YYYY-MM-DD HH24:MI:SSOF",
                                         hint_name='datetimeformattz')

        def validate_datetimeformat() -> HintDateTimeFormat:
            valid_dateformats = VALID_DATETIMEFORMATS
            return validate_literal(valid_dateformats,
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
        datetimeformattz = validator.hintdatetimeformattz.validate()
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
