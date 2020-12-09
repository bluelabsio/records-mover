from typing_inspect import get_args
from typing import Mapping, List
from typing_extensions import Literal
# TypedDict isn't mypy specific, but typing_inspect currently doesn't
# support typing_extensions.TypedDict.
#
# https://github.com/ilevkivskyi/typing_inspect/issues/50
from mypy_extensions import TypedDict

HintEncoding = Literal["UTF8", "UTF16", "UTF16LE", "UTF16BE",
                       "UTF16BOM", "UTF8BOM", "LATIN1", "CP1252"]

HintQuoting = Literal["all", "minimal", "nonnumeric", None]

HintEscape = Literal["\\", None]

HintCompression = Literal['GZIP', 'BZIP', 'LZO', None]

# The trick here works on Literal[True, False] but not on bool:
#
# https://github.com/python/mypy/issues/6366#issuecomment-560369716
HintHeaderRow = Literal[True, False]

HintDoublequote = Literal[True, False]

HintDateFormat = str

HintTimeOnlyFormat = str

HintDateTimeFormatTz = str

HintDateTimeFormat = str

HintFieldDelimiter = str

HintRecordTerminator = str

HintQuoteChar = str

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

HINT_NAMES: List[HintName] = list(get_args(HintName))

PartialRecordsHints = TypedDict('PartialRecordsHints',
                                {
                                    'quoting': HintQuoting,
                                    'header-row': HintHeaderRow,
                                    'field-delimiter': HintFieldDelimiter,
                                    'encoding': HintEncoding,
                                    'escape': HintEscape,
                                    'compression': HintCompression,
                                    'record-terminator': HintRecordTerminator,
                                    'quotechar': HintQuoteChar,
                                    'doublequote': HintDoublequote,
                                    'dateformat': HintDateFormat,
                                    'timeonlyformat': HintTimeOnlyFormat,
                                    'datetimeformattz': HintDateTimeFormatTz,
                                    'datetimeformat': HintDateTimeFormat,
                                },
                                total=False)
UntypedRecordsHints = Mapping[str, object]
