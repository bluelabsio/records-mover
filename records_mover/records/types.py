from typing import Dict, List
from typing_extensions import Literal, TypedDict


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
