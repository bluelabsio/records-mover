from typing import Dict, Optional, Union, List, Mapping
from records_mover.mover_types import JsonValue
from typing_extensions import Literal, TypedDict


"""RecordsHints are described as part of the overall `records format
documentation
<https://github.com/bluelabsio/knowledge/blob/master/Engineering/Architecture/JobDataExchange/output-design.md#hints>`_
Their goal in life is to both describe in detail how to write a
records file (especially delimited files, which are particularly
thorny in practice).

See the :py:meth:`records_mover.records.records_format.RecordsFormat`
for the other details that are typically provided along with records
hints.
"""

RecordsValue = Optional[Union[bool, str]]
RecordsHints = Mapping[str, JsonValue]
MutableRecordsHints = Dict[str, JsonValue]


# https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_TABLE.html
class RecordsManifestEntryMetadata(TypedDict):
    content_length: int


class BaseRecordsManifestEntry(TypedDict):
    url: str
    mandatory: bool


class LegacyRecordsManifestEntry(BaseRecordsManifestEntry, total=False):
    meta: RecordsManifestEntryMetadata


class RecordsManifestEntryWithLength(BaseRecordsManifestEntry):
    meta: RecordsManifestEntryMetadata


class LegacyRecordsManifest(TypedDict):
    entries: List[LegacyRecordsManifestEntry]


class RecordsManifestWithLength(TypedDict):
    entries: List[RecordsManifestEntryWithLength]


class UrlDetailsEntry(TypedDict):
    content_length: int


Url = str
UrlDetails = Dict[Url, UrlDetailsEntry]


#
# Note: Any expansion of these types should also be done in
# records.jobs.hints
#
RecordsFormatType = Literal['delimited', 'parquet']

VALID_VARIANTS = ['dumb', 'csv', 'bigquery', 'bluelabs', 'vertica']
DelimitedVariant = Literal['dumb', 'csv', 'bigquery', 'bluelabs', 'vertica']

HintEncoding = Literal["UTF8", "UTF16", "UTF16LE", "UTF16BE",
                       "UTF16BOM", "UTF8BOM", "LATIN1", "CP1252"]

HintQuoting = Literal["all", "minimal", "nonnumeric", None]

HintEscape = Literal["\\", None]

# TODO: combine this and cli thingie
HintCompression = Literal['GZIP', 'BZIP', 'LZO', None]

# The trick here works on Literal[True, False] but not on bool:
#
# https://github.com/python/mypy/issues/6366#issuecomment-560369716
HintHeaderRow = Literal[True, False]

HintDoublequote = Literal[True, False]


# TODO: This None is a bug in the spec, right?
HintDateFormat = Literal[None, 'YYYY-MM-DD', 'MM-DD-YYYY', 'DD-MM-YYYY', 'MM/DD/YY']

HintTimeOnlyFormat = Literal["HH12:MI AM", "HH24:MI:SS"]

HintDateTimeFormatTz = Literal["YYYY-MM-DD HH:MI:SSOF",
                               "YYYY-MM-DD HH:MI:SS",
                               "YYYY-MM-DD HH24:MI:SSOF",  # TODO: this is listed twice - bug in spec?
                               "YYYY-MM-DD HH24:MI:SSOF",
                               "MM/DD/YY HH24:MI"]

HintDateTimeFormat = Literal["YYYY-MM-DD HH24:MI:SS",
                             'YYYY-MM-DD HH:MI:SS',  # TODO this isn't in spec valid, but is part of a variant
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
                                      },
                                      total=False)


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
