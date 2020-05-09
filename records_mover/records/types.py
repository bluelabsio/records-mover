from typing import Dict, Optional, Union, List, Mapping, Generic, TypeVar, Type
from records_mover.types import JsonValue
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
