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
RecordsFormatType.__doc__ = """Valid values for the general format of records data file ("records
format type")."""

DelimitedVariant = Literal['dumb', 'csv', 'bigquery', 'bluelabs', 'vertica']
DelimitedVariant.__doc__ = """Valid values for the variant of a delimited records format.
Variants specify a default set of parsing hints for how the delimited
file is formatted.  See the `records format specification
<https://github.com/bluelabsio/records-mover/blob/master/docs/RECORDS_SPEC.md>`_
for semantics of each."""
