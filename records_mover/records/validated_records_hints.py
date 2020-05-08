from typing_inspect import is_literal_type, get_args
from typing import NamedTuple, Union, Optional, TypeVar, List, Type, Collection, Generic, Mapping
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


HintName = Literal["header-row",
                   "field-delimiter",
                   "compression",
                   "record-terminator",
                   "quoting",
                   "quotechar",
                   "doublequote",
                   "escape",
                   "encoding",
                   "dateformat",
                   "timeonlyformat",
                   "datetimeformattz",
                   "datetimeformat"]


class Hint(Generic[HintT]):
    def __init__(self,
                 type_: Type[HintT],
                 hint_name: HintName,
                 default: HintT):
        self.default = default
        self.type_ = type_
        self.hint_name = hint_name
        self.valid_values: List[HintT] = list(get_args(type_))


class Hints:
    # mypy gives this when we pass the HintBlahBlah aliases in as an
    # argument here:
    #
    # error: The type alias to Union is invalid in runtime context
    #
    # Nonetheless, the validation works.
    datetimeformattz = Hint[HintDateTimeFormatTz](HintDateTimeFormatTz,  # type: ignore
                                                  "datetimeformattz",
                                                  "YYYY-MM-DD HH24:MI:SSOF")
    datetimeformat = Hint[HintDateTimeFormat](HintDateTimeFormat,  # type: ignore
                                              "datetimeformat",
                                              default="YYYY-MM-DD HH24:MI:SS")  # TODO: test to see if default is typesafe
    compression = Hint[HintCompression](HintCompression,  # type: ignore
                                        'compression',
                                        default=None)
    quoting = Hint[HintQuoting](HintQuoting,  # type: ignore
                                'quoting',
                                default='minimal')
    escape = Hint[HintEscape](HintEscape,  # type: ignore
                              'escape',
                              default='\\')
    encoding = Hint[HintEncoding](HintEncoding,  # type: ignore
                                  'encoding',
                                  default='UTF8')
    dateformat = Hint[HintDateFormat](HintDateFormat,  # type: ignore
                                      'dateformat',
                                      default='YYYY-MM-DD')
    timeonlyformat = Hint[HintTimeOnlyFormat](HintTimeOnlyFormat,  # type: ignore
                                              'timeonlyformat',
                                              default="HH24:MI:SS")


HintA = TypeVar('HintA')

HintB = TypeVar('HintB')


class HintValidator:
    def __init__(self,
                 fail_if_cant_handle_hint: bool,
                 hints: RecordsHints):
        self.fail_if_cant_handle_hint = fail_if_cant_handle_hint
        self.hints = hints

    def validate_literal(self, hint: Hint[HintA]) -> HintA:
        # MyPy doesn't like looking for a generic optional string
        # in a list of specific optional strings.  It's wrong;
        # that's perfectly safe
        x: object = self.hints[hint.hint_name]
        try:
            i = hint.valid_values.index(x)  # type: ignore
            return hint.valid_values[i]
        except ValueError:
            cant_handle_hint(fail_if_cant_handle_hint=self.fail_if_cant_handle_hint,
                             hint_name=hint.hint_name,
                             hints=self.hints)
            return hint.default

    # TODO: Can this method be typesafe?  HintT doesn't even mean anything here
    def validate_hint(self, hint: Hint[HintB]) -> HintB:
        assert is_literal_type(hint.type_)

        return self.validate_literal(hint)

    def validate(self) -> 'ValidatedRecordsHints':
        def validate_boolean(hint_name: str) -> Union[Literal[True], Literal[False]]:
            x = self.hints[hint_name]
            if x is True:
                return True
            if x is False:
                return False
            cant_handle_hint(fail_if_cant_handle_hint=self.fail_if_cant_handle_hint,
                             hint_name=hint_name,
                             hints=self.hints)
            return True

        def validate_string(hint_name: str) -> str:
            x = self.hints[hint_name]
            if isinstance(x, str):
                return x
            cant_handle_hint(fail_if_cant_handle_hint=self.fail_if_cant_handle_hint,
                             hint_name=hint_name,
                             hints=self.hints)
            return str(x)

        def validate_optional_string(hint_name: str) -> Optional[str]:
            x = self.hints[hint_name]
            if x is None:
                return x
            if isinstance(x, str):
                return x
            cant_handle_hint(fail_if_cant_handle_hint=self.fail_if_cant_handle_hint,
                             hint_name=hint_name,
                             hints=self.hints)
            return str(x)

        header_row = validate_boolean('header-row')
        field_delimiter = validate_string('field-delimiter')
        record_terminator = validate_string('record-terminator')
        quotechar = validate_string('quotechar')
        doublequote = validate_boolean('doublequote')
        # TODO: After one create functions
        return ValidatedRecordsHints(
            header_row=header_row,
            field_delimiter=field_delimiter,
            compression=self.validate_hint(Hints.compression),
            record_terminator=record_terminator,
            quoting=self.validate_hint(Hints.quoting),
            quotechar=quotechar,
            doublequote=doublequote,
            escape=self.validate_hint(Hints.escape),
            encoding=self.validate_hint(Hints.encoding),
            dateformat=self.validate_hint(Hints.dateformat),
            timeonlyformat=self.validate_hint(Hints.timeonlyformat),
            datetimeformattz=self.validate_hint(Hints.datetimeformattz),
            datetimeformat=self.validate_hint(Hints.datetimeformat), # TODO: Should fail, should not be tz
        )


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
        return validator.validate()
