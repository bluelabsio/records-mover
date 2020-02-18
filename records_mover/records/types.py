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
    BootstrappingRecordsHints = TypedDict('BootstrappingRecordsHints',
                                          {
                                              'quoting': Optional[str],
                                              'header-row': bool,
                                              'field-delimiter': str,
                                              'encoding': str,
                                              'escape': Optional[str],
                                              'compression': Optional[str],
                                          },
                                          total=False)

    RecordsFormatType = Literal['delimited', 'parquet']
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

    BootstrappingRecordsHints = RecordsHints

    RecordsFormatType = str
