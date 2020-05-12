from .hint import LiteralHint, StringHint
from .types import (
    HintHeaderRow, HintCompression, HintQuoting,
    HintDoublequote, HintEscape, HintEncoding, HintDateFormat, HintTimeOnlyFormat,
    HintDateTimeFormatTz, HintDateTimeFormat
)
import logging


logger = logging.getLogger(__name__)


class Hints:
    # mypy gives this when we pass the HintBlahBlah aliases in as an
    # argument here:
    #
    # error: The type alias to Union is invalid in runtime context
    #
    # Nonetheless, the validation works.
    datetimeformattz = LiteralHint[HintDateTimeFormatTz](HintDateTimeFormatTz,  # type: ignore
                                                         "datetimeformattz",
                                                         "YYYY-MM-DD HH24:MI:SSOF")
    datetimeformat = LiteralHint[HintDateTimeFormat](HintDateTimeFormat,  # type: ignore
                                                     "datetimeformat",
                                                     default="YYYY-MM-DD HH24:MI:SS")
    compression = LiteralHint[HintCompression](HintCompression,  # type: ignore
                                               'compression',
                                               default=None)
    quoting = LiteralHint[HintQuoting](HintQuoting,  # type: ignore
                                       'quoting',
                                       default='minimal')
    escape = LiteralHint[HintEscape](HintEscape,  # type: ignore
                                     'escape',
                                     default='\\')
    encoding = LiteralHint[HintEncoding](HintEncoding,  # type: ignore
                                         'encoding',
                                         default='UTF8')
    dateformat = LiteralHint[HintDateFormat](HintDateFormat,  # type: ignore
                                             'dateformat',
                                             default='YYYY-MM-DD')
    timeonlyformat = LiteralHint[HintTimeOnlyFormat](HintTimeOnlyFormat,  # type: ignore
                                                     'timeonlyformat',
                                                     default="HH24:MI:SS")
    doublequote = LiteralHint[HintDoublequote](HintDoublequote,  # type: ignore
                                               'doublequote',
                                               default=False)
    header_row = LiteralHint[HintHeaderRow](HintHeaderRow,  # type: ignore
                                            'header-row',
                                            default=True)
    quotechar = StringHint('quotechar', default='"')
    record_terminator = StringHint('record-terminator', default='\n')
    field_delimiter = StringHint('field-delimiter', default=',')
