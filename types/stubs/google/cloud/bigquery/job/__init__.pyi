# https://github.com/googleapis/google-cloud-python/blob/de73e45a7183a638113153d0faec105cfc437f0e/bigquery/google/cloud/bigquery/job.py

from typing import Union, List, Optional, Sequence, Mapping, Any, Dict
from typing_extensions import Literal
import google.cloud.bigquery.job
import google.cloud.bigquery.schema
import google.cloud.bigquery.table
import google.cloud.bigquery.external_config
import google.cloud.bigquery.encryption_configuration
import google.api_core.retry
from google.cloud.bigquery.retry import DEFAULT_RETRY


class CreateDisposition:
    CREATE_IF_NEEDED: str
    CREATE_NEVER: str


class WriteDisposition:
    WRITE_APPEND: str
    WRITE_TRUNCATE: str
    WRITE_EMPTY: str


class Encoding:
    ...


class SchemaUpdateOption:
    ...


class SourceFormat:
    ...


class _AsyncJob:
    ...


class LoadJob:
    def result(self,
               retry: google.api_core.retry.Retry = DEFAULT_RETRY,
               timeout: Optional[float] = None) -> _AsyncJob:
        ...

    errors: Optional[List[Mapping[Any, Any]]]
    output_rows: Optional[int]


class ExtractJob:
    def result(self,
               retry: google.api_core.retry.Retry = DEFAULT_RETRY,
               timeout: Optional[float] = None) -> _AsyncJob:
        ...


class LoadJobConfig:
    allow_jagged_rows: bool
    allow_quoted_newlines: bool
    autodetect: bool
    clustering_fields: Union[List[str], None]
    create_disposition: str
    destination_encryption_configuration:\
        Optional[google.cloud.bigquery.encryption_configuration.EncryptionConfiguration]
    destination_table_description: Union[str, None]
    destination_table_friendly_name: Union[str, None]
    encoding: str
    field_delimiter: str
    hive_partitioning: Optional[google.cloud.bigquery.external_config.HivePartitioningOptions]
    ignore_unknown_values: bool
    max_bad_records: int
    null_marker: str
    quote_character: str
    range_partitioning: Optional[google.cloud.bigquery.table.RangePartitioning]
    schema: Sequence[Union[google.cloud.bigquery.schema.SchemaField, Mapping[str, Any]]]
    schema_update_options: Optional[List[google.cloud.bigquery.job.SchemaUpdateOption]]
    skip_leading_rows: int
    source_format: str
    time_partitioning: google.cloud.bigquery.table.TimePartitioning
    use_avro_logical_types: bool
    write_disposition: str

    def to_api_repr(self) -> str:
        ...


# https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.ExtractJobConfig.html#google.cloud.bigquery.job.ExtractJobConfig.compression
class ExtractJobConfig:
    compression: Literal['GZIP', 'DEFLATE', 'SNAPPY', 'NONE']
    destination_format: Literal['CSV', 'NEWLINE_DELIMITED_JSON', 'AVRO']
    field_delimeter: str
    labels: Dict[str, str]
