from typing import Dict, List
from typing_extensions import Literal, TypedDict
from typing_inspect import get_args


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


RecordsFormatType = Literal['avro', 'delimited', 'parquet']

RECORDS_FORMAT_TYPES: List[str] = list(get_args(RecordsFormatType))

DelimitedVariant = Literal['dumb', 'csv', 'bigquery', 'bluelabs', 'vertica']
