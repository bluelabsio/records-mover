from .hint import LiteralHint, StringHint, Hint
from .types import (
    HintHeaderRow, HintCompression, HintQuoting,
    HintDoublequote, HintEscape, HintEncoding,
    UntypedRecordsHints, PartialRecordsHints
)
from enum import Enum
import logging


logger = logging.getLogger(__name__)


class Hints(Enum):
    value: Hint
    datetimeformattz = StringHint("datetimeformattz",
                                  "YYYY-MM-DD HH24:MI:SSOF",
                                  description=("Format used to write "
                                               "'datetimetz' values"))
    datetimeformat = StringHint("datetimeformat",
                                default="YYYY-MM-DD HH24:MI:SS",
                                description=("Format used to write "
                                             "'datetime' values"))
    # mypy gives this when we pass the HintBlahBlah aliases in as an
    # argument here:
    #
    # error: The type alias to Union is invalid in runtime context
    #
    # Nonetheless, the validation works.
    compression = LiteralHint[HintCompression](HintCompression,  # type: ignore
                                               'compression',
                                               default=None,
                                               description='Compression type of the file.')
    quoting = LiteralHint[HintQuoting](HintQuoting,  # type: ignore
                                       'quoting',
                                       default='minimal',
                                       description=('How quotes are applied to individual fields. '
                                                    'all: quote all fields. '
                                                    'minimal: quote only fields that contain '
                                                    'ambiguous characters (the '
                                                    'delimiter, the escape character, or a line '
                                                    'terminator). '
                                                    'default: never quote fields.'))
    escape = LiteralHint[HintEscape](HintEscape,  # type: ignore
                                     'escape',
                                     default='\\',
                                     description="Character used to escape strings")
    encoding = LiteralHint[HintEncoding](HintEncoding,  # type: ignore
                                         'encoding',
                                         default='UTF8',
                                         description="Text encoding of file")
    dateformat = StringHint('dateformat',
                            default='YYYY-MM-DD',
                            description=("Format used to write "
                                         "'date' values"))
    timeonlyformat = StringHint('timeonlyformat',
                                default="HH24:MI:SS",
                                description=("Format used to write "
                                             "'time' values"))
    # https://docs.python.org/3/library/csv.html#csv.Dialect.doublequote
    doublequote = LiteralHint[HintDoublequote](HintDoublequote,  # type: ignore
                                               'doublequote',
                                               default=False,
                                               description=('Controls how instances of quotechar '
                                                            'appearing inside a field should '
                                                            'themselves be quoted. When True, the '
                                                            'character is doubled. When False, the '
                                                            'escapechar is used as a prefix to the '
                                                            'quotechar.'))
    header_row = LiteralHint[HintHeaderRow](HintHeaderRow,  # type: ignore
                                            'header-row',
                                            default=True,
                                            description=('True if a header row is provided in '
                                                         'the delimited files.'))
    # https://docs.python.org/3/library/csv.html#csv.Dialect.quotechar
    quotechar = StringHint('quotechar',
                           default='"',
                           description=('A one-character string used to quote fields containing '
                                        'special characters, such as the delimiter or quotechar, '
                                        'or which contain new-line characters.'))
    record_terminator = StringHint('record-terminator',
                                   default='\n',
                                   description='String used to close out individual rows of data.')
    field_delimiter = StringHint('field-delimiter',
                                 default=',',
                                 description='Character used between fields.')


def validate_partial_hints(untyped_hints: UntypedRecordsHints,
                           fail_if_cant_handle_hint: bool) -> PartialRecordsHints:
    typed_records_hints: PartialRecordsHints = {}
    for key in untyped_hints:
        hint_obj = Hints[key.replace('-', '_')].value
        value = hint_obj.validate(untyped_hints,
                                  fail_if_cant_handle_hint=fail_if_cant_handle_hint)
        typed_records_hints[key] = value  # type: ignore
    return typed_records_hints
