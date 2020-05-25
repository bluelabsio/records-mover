from typing_inspect import get_args, typed_dict_keys
from typing import Optional, Union, Mapping, Dict, List
from typing_extensions import Literal
# TypedDict isn't mypy specific, but typing_inspect currently doesn't
# support typing_extensions.TypedDict.
#
# https://github.com/ilevkivskyi/typing_inspect/issues/50
from mypy_extensions import TypedDict
from records_mover.mover_types import JsonValue


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

HintDateFormat = Literal['YYYY-MM-DD', 'MM-DD-YYYY', 'DD-MM-YYYY', 'MM/DD/YY']

HintTimeOnlyFormat = Literal["HH12:MI AM", "HH24:MI:SS"]

HintDateTimeFormatTz = Literal["YYYY-MM-DD HH:MI:SSOF",
                               "YYYY-MM-DD HH:MI:SS",
                               "YYYY-MM-DD HH24:MI:SSOF",
                               "MM/DD/YY HH24:MI"]

HintDateTimeFormat = Literal["YYYY-MM-DD HH24:MI:SS",
                             'YYYY-MM-DD HH:MI:SS',
                             "YYYY-MM-DD HH12:MI AM",
                             "MM/DD/YY HH24:MI"]

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

HINT_NAMES: List[HintName] = list(get_args(HintName))  # type: ignore

# TODO: Retire this
BootstrappingRecordsHints = TypedDict('BootstrappingRecordsHints',
                                      {
                                          'quoting': HintQuoting,
                                          'header-row': HintHeaderRow,
                                          'field-delimiter': HintFieldDelimiter,
                                          'encoding': HintEncoding,
                                          'escape': HintEscape,
                                          'compression': HintCompression,
                                          'record-terminator': HintRecordTerminator,
                                      },
                                      total=False)

# TODO: Retire this
BOOTSTRAPPING_HINT_NAMES: List[HintName] = typed_dict_keys(BootstrappingRecordsHints)

TypedRecordsHints = TypedDict('TypedRecordsHints',
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
RecordsHints = Mapping[str, object]  # TODO: make this the typed one
UntypedRecordsHints = Mapping[str, object]
MutableRecordsHints = Dict[str, object]  # TODO: make this the typed one
MutableUntypedRecordsHints = Dict[str, object]
