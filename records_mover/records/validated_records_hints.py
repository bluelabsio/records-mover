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

# TODO: these should live in hints.py - existing stuff should probably move
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
                                              default="YYYY-MM-DD HH24:MI:SS")
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
    doublequote = Hint[HintDoublequote](HintDoublequote,  # type: ignore
                                        'doublequote',
                                        default=False)
    header_row = Hint[HintHeaderRow](HintHeaderRow,  # type: ignore
                                     'header-row',
                                     default=True)
    quotechar = Hint[HintQuoteChar](HintQuoteChar,
                                    'quotechar',
                                    default='"')
    record_terminator = Hint[HintRecordTerminator](HintRecordTerminator,
                                                   'record-terminator',
                                                   default='\n')
    field_delimiter = Hint[HintFieldDelimiter](HintFieldDelimiter,
                                               'field-delimiter',
                                               default=',')


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
            # well, sort of safe.
            cant_handle_hint(fail_if_cant_handle_hint=self.fail_if_cant_handle_hint,
                             hint_name=hint.hint_name,
                             hints=self.hints)
            return hint.default

    def validate_str(self, hint: Hint[str]) -> str:
        x: object = self.hints[hint.hint_name]
        if isinstance(x, str):
            return x
        else:
            cant_handle_hint(fail_if_cant_handle_hint=self.fail_if_cant_handle_hint,
                             hint_name=hint.hint_name,
                             hints=self.hints)
            return hint.default

    # TODO: Can this method be typesafe?  HintT doesn't even mean anything here
    def validate_hint(self, hint: Hint[HintB]) -> HintB:
        if is_literal_type(hint.type_):
            return self.validate_literal(hint)
        # TODO: Can I do this in a specialized subtype?
        elif hint.type_ == str:
            return self.validate_str(hint)  # type: ignore
        else:
            raise NotImplementedError(f"Teach me how to validate {hint.type_}")

    def validate(self) -> 'ValidatedRecordsHints':
        def validate_string(hint_name: str) -> str:
            x = self.hints[hint_name]
            if isinstance(x, str):
                return x
            cant_handle_hint(fail_if_cant_handle_hint=self.fail_if_cant_handle_hint,
                             hint_name=hint_name,
                             hints=self.hints)
            return str(x)

        return ValidatedRecordsHints(
            header_row=self.validate_hint(Hints.header_row),
            field_delimiter=self.validate_hint(Hints.field_delimiter),
            compression=self.validate_hint(Hints.compression),
            record_terminator=self.validate_hint(Hints.record_terminator),
            quoting=self.validate_hint(Hints.quoting),
            quotechar=self.validate_hint(Hints.quotechar),
            doublequote=self.validate_hint(Hints.doublequote),
            escape=self.validate_hint(Hints.escape),
            encoding=self.validate_hint(Hints.encoding),
            dateformat=self.validate_hint(Hints.dateformat),
            timeonlyformat=self.validate_hint(Hints.timeonlyformat),
            datetimeformattz=self.validate_hint(Hints.datetimeformattz),
            datetimeformat=self.validate_hint(Hints.datetimeformat),
        )


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
