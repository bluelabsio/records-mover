from typing import Dict, Optional, Union, List, Mapping, NamedTuple, Any, TypeVar, TYPE_CHECKING
from records_mover.types import JsonValue

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


if TYPE_CHECKING:
    from ..db import DBDriver  # noqa
    # https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_TABLE.html
    from mypy_extensions import TypedDict

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

    from typing_extensions import Literal  # noqa

    from mypy_extensions import TypedDict

    #
    # Note: Any expansion of these types should also be done in
    # records.jobs.hints
    #
    RecordsFormatType = Literal['delimited', 'parquet']

    DelimitedVariant = Literal['dumb', 'csv', 'bigquery', 'bluelabs', 'vertica']

else:
    RecordsManifestEntryMetadata = Mapping[str, int]
    LegacyRecordsManifestEntry = Mapping[str, Union[str, bool, int, RecordsManifestEntryMetadata]]
    RecordsManifestEntryWithLength =\
        Mapping[str, Union[str, bool, int, RecordsManifestEntryMetadata]]
    LegacyRecordsManifest = Mapping[str, List[LegacyRecordsManifestEntry]]
    RecordsManifestWithLength = Mapping[str, List[RecordsManifestEntryWithLength]]
    UrlDetailsEntry = Dict[str, int]
    Url = str
    UrlDetails = Dict[Url, UrlDetailsEntry]

    RecordsFormatType = str

    DelimitedVariant = str

if TYPE_CHECKING:
    HintEncoding = Literal["UTF8", "UTF16", "UTF16LE", "UTF16BE",
                           "UTF16BOM", "UTF8BOM", "LATIN1", "CP1252"]
else:
    HintEncoding = str

VALID_ENCODING: List[HintEncoding] = [
    "UTF8", "UTF16", "UTF16LE", "UTF16BE", "UTF16BOM", "UTF8BOM", "LATIN1", "CP1252"
]

if TYPE_CHECKING:
    HintQuoting = Literal["all", "minimal", "nonnumeric", None]
else:
    HintQuoting = Optional[str]

VALID_QUOTING: List[HintQuoting] = ["all", "minimal", "nonnumeric", None]


if TYPE_CHECKING:
    HintEscape = Literal["\\", None]
else:
    HintEscape = Optional[str]

VALID_ESCAPE: List[HintEscape] = ["\\", None]

# TODO: combine this and cli thingie
if TYPE_CHECKING:
    HintCompression = Literal['GZIP', 'BZIP', 'LZO', None]
else:
    HintCompression = Optional[str]

VALID_COMPRESSIONS: List[HintCompression] = ['GZIP', 'BZIP', 'LZO', None]

if TYPE_CHECKING:
    # The trick here works on Literal[True, False] but not on bool:
    #
    # https://github.com/python/mypy/issues/6366#issuecomment-560369716
    HintHeaderRow = Literal[True, False]

    HintDoublequote = Literal[True, False]
else:
    HintHeaderRow = bool

    HintDoublequote = bool


if TYPE_CHECKING:
    # TODO: This None is a bug in the spec, right?
    HintDateFormat = Literal[None, 'YYYY-MM-DD', 'MM-DD-YYYY', 'DD-MM-YYYY', 'MM/DD/YY']
else:
    # TODO: This None is a bug in the spec, right?
    HintDateFormat = Optional[str]

VALID_DATEFORMATS: List[HintDateFormat] = [
    None, 'YYYY-MM-DD', 'MM-DD-YYYY', 'DD-MM-YYYY', 'MM/DD/YY'
]

if TYPE_CHECKING:
    HintTimeOnlyFormat = Literal["HH12:MI AM", "HH24:MI:SS"]
else:
    HintTimeOnlyFormat = str

VALID_TIMEONLYFORMATS: List[HintTimeOnlyFormat] = ["HH12:MI AM", "HH24:MI:SS"]

if TYPE_CHECKING:
    HintDateTimeFormatTz = Literal["YYYY-MM-DD HH:MI:SSOF",
                                   "YYYY-MM-DD HH:MI:SS",
                                   "YYYY-MM-DD HH24:MI:SSOF", # TODO: this is listed twice - bug in spec?
                                   "YYYY-MM-DD HH24:MI:SSOF",
                                   "MM/DD/YY HH24:MI"]
else:
    HintDateTimeFormatTz = str

VALID_DATETIMEFORMATTZS: List[HintDateTimeFormatTz] = ["YYYY-MM-DD HH:MI:SSOF",
                                                       "YYYY-MM-DD HH:MI:SS",
                                                       "YYYY-MM-DD HH24:MI:SSOF", # TODO: this is listed twice - bug in spec?
                                                       "YYYY-MM-DD HH24:MI:SSOF",
                                                       "MM/DD/YY HH24:MI"]


if TYPE_CHECKING:
    HintDateTimeFormat = Literal["YYYY-MM-DD HH24:MI:SS",
                                 'YYYY-MM-DD HH:MI:SS', # TODO this isn't in spec valid, but is part of a variant
                                 "YYYY-MM-DD HH12:MI AM",
                                 "MM/DD/YY HH24:MI"]
else:
    HintDateTimeFormat = str


VALID_DATETIMEFORMATS: List[HintDateTimeFormat] = ["YYYY-MM-DD HH24:MI:SS",
                                                   'YYYY-MM-DD HH:MI:SS', # TODO this isn't in spec valid, but is part of a variant
                                                   "YYYY-MM-DD HH12:MI AM",
                                                   "MM/DD/YY HH24:MI"]

HintFieldDelimiter = str

HintRecordTerminator = str

HintQuoteChar = str


if TYPE_CHECKING:
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

else:
    BootstrappingRecordsHints = RecordsHints


INVALID_OBJECT = object()
