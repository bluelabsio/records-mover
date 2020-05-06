from typing import Dict, Optional, Union, List, Mapping, TYPE_CHECKING

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
RecordsHints = Dict[str, Optional[Union[bool, str]]]

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

    HintEncoding = str

    HintQuoting = Optional[str]

    HintEscape = Optional[str]

    HintCompression = Optional[str]

    HintHeaderRow = bool

    HintDoublequote = bool

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
