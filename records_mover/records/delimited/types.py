from typing import Optional, Union, Mapping, Dict
from typing_extensions import Literal, TypedDict
from records_mover.mover_types import JsonValue


RecordsValue = Optional[Union[bool, str]]
RecordsHints = Mapping[str, JsonValue]
MutableRecordsHints = Dict[str, JsonValue]

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
