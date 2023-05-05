from ...utils import quiet_remove
from ...records.delimited import cant_handle_hint
from typing import Set
from ...records.load_plan import RecordsLoadPlan
from ...records.records_format import (
    DelimitedRecordsFormat, ParquetRecordsFormat, AvroRecordsFormat
)
from records_mover.records.delimited import ValidatedRecordsHints
from records_mover.mover_types import _assert_never
from google.cloud.bigquery.job import CreateDisposition, WriteDisposition
from google.cloud import bigquery
import logging

logger = logging.getLogger(__name__)


def load_job_config(unhandled_hints: Set[str],
                    load_plan: RecordsLoadPlan) -> bigquery.LoadJobConfig:

    # https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#type_conversions
    # https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.job.LoadJobConfig.html
    # https://cloud.google.com/bigquery/docs/reference/rest/v2/JobConfiguration#JobConfigurationTableCopy

    fail_if_cant_handle_hint = load_plan.processing_instructions.fail_if_cant_handle_hint
    config = bigquery.LoadJobConfig()

    # clustering_fields: Fields defining clustering for the table
    #
    # > Currently, BigQuery supports clustering over a partitioned
    # > table.  Use clustering over a partitioned table when:
    # > * Your data is already partitioned on a date or timestamp column.
    # > * You commonly use filters or aggregation against
    # >   particular columns in your queries.
    #
    # https://cloud.google.com/bigquery/docs/clustered-tables
    config.clustering_fields = None

    # autodetect: Automatically infer the schema from a sample of the data.
    # schema: Schema of the destination table.
    # create_disposition: Specifies behavior for creating tables.
    #
    # Rely on prep.py in records/ to create the table.
    config.autodetect = False
    config.create_disposition = CreateDisposition.CREATE_NEVER

    # destination_encryption_configuration: Custom encryption configuration for
    # the destination table.
    #
    # Custom encryption configuration (e.g., Cloud KMS keys) or
    # None if using default encryption.
    config.destination_encryption_configuration = None

    # destination_table_description: Union[str, None] name given to destination
    # table.
    config.destination_table_description = None

    # destination_table_friendly_name: Union[str, None] name given
    # to destination table.
    config.destination_table_friendly_name = None

    # ignore_unknown_values: Ignore extra values not represented
    # in the table schema
    config.ignore_unknown_values = load_plan.processing_instructions.fail_if_row_invalid

    # max_bad_records: Number of invalid rows to ignore.
    if load_plan.processing_instructions.max_failure_rows is not None:
        config.max_bad_records = load_plan.processing_instructions.max_failure_rows
        config.allow_jagged_rows = True
    elif load_plan.processing_instructions.fail_if_row_invalid:
        config.max_bad_records = 0
        config.allow_jagged_rows = False
    else:
        config.max_bad_records = 999999
        config.allow_jagged_rows = True

    # write_disposition: Action that occurs if the destination
    # table already exists.
    #
    # Since prep.py handles whatever policy, the table will be
    # already empty if we don't want to append anyway:
    config.write_disposition = WriteDisposition.WRITE_APPEND

    # time_partitioning: Specifies time-based partitioning for the
    # destination table.

    # use_avro_logical_types: For loads of Avro data, governs whether Avro
    # logical types are converted to their corresponding BigQuery types

    # labels: Labels for the job.
    #
    # This method always returns a dict. To change a job’s labels,
    # modify the dict, then call Client.update_job. To delete a
    # label, set its value to None before updating.

    # schema_update_options: Specifies updates to the destination
    # table schema to allow as a side effect of the load job.
    #
    # Allows the schema of the destination table to be updated as
    # a side effect of the query job. Schema update options are
    # supported in two cases: when writeDisposition is
    # WRITE_APPEND; when writeDisposition is WRITE_TRUNCATE and
    # the destination table is a partition of a table, specified
    # by partition decorators. For normal tables, WRITE_TRUNCATE
    # will always overwrite the schema. One or more of the
    # following values are specified: ALLOW_FIELD_ADDITION: allow
    # adding a nullable field to the
    # schema. ALLOW_FIELD_RELAXATION: allow relaxing a required
    # field in the original schema to nullable.
    config.schema_update_options = None
    fail_if_cant_handle_hint = load_plan.processing_instructions.fail_if_cant_handle_hint
    if isinstance(load_plan.records_format, DelimitedRecordsFormat):
        hints = load_plan.records_format.validate(fail_if_cant_handle_hint=fail_if_cant_handle_hint)
        add_load_job_csv_config(unhandled_hints,
                                hints,
                                fail_if_cant_handle_hint,
                                config)
        return config

    if isinstance(load_plan.records_format, ParquetRecordsFormat):
        config.source_format = 'PARQUET'
        return config

    if isinstance(load_plan.records_format, AvroRecordsFormat):
        config.source_format = 'AVRO'
        # https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#logical_types
        config.use_avro_logical_types = True
        return config

    raise NotImplementedError("Not currently able to load "
                              f"{load_plan.records_format.format_type}")


def add_load_job_csv_config(unhandled_hints: Set[str],
                            hints: ValidatedRecordsHints,
                            fail_if_cant_handle_hint: bool,
                            config: bigquery.LoadJobConfig) -> None:
    # source_format: File format of the data.
    config.source_format = 'CSV'

    # encoding: The character encoding of the data.
    # The supported values are UTF-8 or ISO-8859-1.
    # "UTF-8 or ISO-8859-1"
    #
    if hints.encoding == 'UTF8':
        config.encoding = 'UTF-8'
    else:
        # Currently records hints don't support ISO-8859-1
        cant_handle_hint(fail_if_cant_handle_hint, 'encoding', hints)
    quiet_remove(unhandled_hints, 'encoding')

    # field_delimiter: The separator for fields in a CSV file.
    assert isinstance(hints.field_delimiter, str)
    config.field_delimiter = hints.field_delimiter
    quiet_remove(unhandled_hints, 'field-delimiter')

    # allow_jagged_rows: Allow missing trailing optional columns (CSV only).

    # null_marker: Represents a null value (CSV only)
    #
    # (documentation is mangled for this one, but I assume the default is
    # '' or something sensible, so not messing with it)

    # quote_character: Character used to quote data sections (CSV
    # only).
    #
    # [Optional] The value that is used to quote data sections in
    # a CSV file. BigQuery converts the string to ISO-8859-1
    # encoding, and then uses the first byte of the encoded string
    # to split the data in its raw, binary state. The default
    # value is a double-quote ('"'). If your data does not contain
    # quoted sections, set the property value to an empty
    # string. If your data contains quoted newline characters, you
    # must also set the allowQuotedNewlines property to
    # true.
    #
    # @default "

    # I tried a few combinations and found that when you leave quote_character as the default
    #
    # * Fields quoted with "" are loaded without the surrounding quotes in the
    #   string
    # * "" becomes " in a quoted field
    # * "" stays "" in a non-quoted field
    # * nonnumeric quoting works fine
    # * full quoting works fine

    if hints.quoting is None:
        config.quote_character = ''
    elif hints.quoting == 'all' or hints.quoting == 'minimal' or hints.quoting == 'nonnumeric':
        # allow_quoted_newlines: Allow quoted data containing newline
        # characters (CSV only).

        config.allow_quoted_newlines = True

        assert isinstance(hints.quotechar, str)
        config.quote_character = hints.quotechar
        if hints.doublequote:
            pass
        else:
            cant_handle_hint(fail_if_cant_handle_hint, 'doublequote', hints)

    else:
        _assert_never(hints.quoting)
    quiet_remove(unhandled_hints, 'quoting')
    quiet_remove(unhandled_hints, 'quotechar')
    quiet_remove(unhandled_hints, 'doublequote')

    # No mention of escaping in BigQuery documentation, and in
    # practice backslashes come through without being interpreted.
    if hints.escape is None:
        pass
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'escape', hints)
    quiet_remove(unhandled_hints, 'escape')

    # skip_leading_rows: Number of rows to skip when reading data (CSV only).
    if hints.header_row:
        config.skip_leading_rows = 1
    else:
        config.skip_leading_rows = 0
    quiet_remove(unhandled_hints, 'header-row')

    # "When you load CSV or JSON data, values in DATE columns must
    #  use the dash (-) separator and the date must be in the
    # following format: YYYY-MM-DD (year-month-day)."
    if hints.dateformat == 'YYYY-MM-DD':
        pass
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'dateformat', hints)
    quiet_remove(unhandled_hints, 'dateformat')

    # "When you load JSON or CSV data, values in TIMESTAMP columns
    #  must use a dash (-) separator for the date portion of the
    #  timestamp, and the date must be in the following format:
    #  YYYY-MM-DD (year-month-day). The hh:mm:ss
    #  (hour-minute-second) portion of the timestamp must use a
    #  colon (:) separator."
    #
    #
    # To test, log into BigQuery web console and try SQL like this
    #   (assumption is that the same timestamp parser is used during
    #   CSV loads)
    #
    #      select TIMESTAMP("2000-01-02 16:34:56.789012US/Eastern") as a;
    #
    # Tests performed and result displayed on console query:
    #
    # DATE:
    # * 01-02-2019 (rejected):
    # * 01/02/19 (rejected):
    # * 2019-01-01 (accepted): 2019-01-01
    # DATETIME:
    # * 2019-01-01 1:00pm (rejected):
    # * 2019-01-01 1:00:00pm (rejected)
    # * 2019-01-01 1:00PM (rejected):
    # * 2019-01-01 13:00 (rejected):
    # * 2019-01-01 13:00:00 (accepted): 2019-01-01T13:00:00
    # * 2019-01-01 1:00pm US/Eastern (rejected):
    # * 2019-01-01 1:00:00pm US/Eastern (rejected):
    # * 2019-01-01 13:00:00 US/Eastern (rejected):
    # * 2019-01-01 13:00:00 EST (rejected):
    # * 1997-12-17 07:37:16-08 (rejected)
    # * 2019-01-01T13:00:00 (accepted): 2019-01-01T13:00:00
    #
    # TIME:
    # * 1:00pm (rejected):
    # * 1:00:00pm (rejected):
    # * 13:00 (rejected):
    # * 13:00:00 (accepted): 13:00:00
    # * 1:00pm US/Eastern (rejected):
    # * 1:00pm EST (rejected):
    # * 07:37:16-08 (rejected):
    #
    # TIMESTAMP ("Required format is YYYY-MM-DD
    # HH:MM[:SS[.SSSSSS]]", which is BS, as it doesn't specify the
    # timezone format):
    #
    # * 2019-01-01 1:00pm (rejected):
    # * 2019-01-01 1:00:00pm (rejected)
    # * 2019-01-01 1:00PM (rejected):
    # * 2019-01-01 13:00 (rejected):
    # * 2019-01-01 13:00:00 (accepted): 2019-01-01T13:00:00
    # * 2019-01-01 1:00pm US/Eastern (rejected):
    # * 2019-01-01 1:00:00pm US/Eastern (rejected):
    # * 2019-01-01 13:00:00 US/Eastern (rejected):
    # * 2019-01-01 13:00:00 EST (rejected):
    # * 1997-12-17 07:37:16-08 (accepted): 1997-12-17 15:37:16 UTC
    # * 2019-01-01T13:00:00-08 (accepted): 2019-01-01 21:00:00 UTC
    # * 2000-01-02 16:34:56.789012+0000 (rejected)
    # * 2000-01-02 16:34:56.789012+00:00 (accepted)
    # * 2000-01-02 16:34:56.789012EST (rejected)
    # * 2000-01-02 16:34:56.789012US/Eastern (rejected)
    # * 2000-01-02 16:34:56.789012UTC (accepted): 2000-01-02 16:34:56.789012 UTC
    # * 2000-01-02 16:34:56.789012 UTC (accepted: 2000-01-02 16:34:56.789012 UTC
    #
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#examples
    #
    # https://stackoverflow.com/questions/47466296/bigquery-datetime-format-csv-to-bigquery-yyyy-mm-dd-hhmmss-ssssss
    #
    # BigQuery supports exactly one format of ingesting timestamps
    # with timezones (what they call 'TIMESTAMP' they call timestamps
    # without timezones 'DATETIME'.
    #
    # That format they accept is ISO 8601, which sounds all nice and
    # standardy. Usable timestamps look like 2000-01-02
    # 16:34:56.789012+00:00.
    # Cool cool. The only issue is that Python's strftime doesn't
    # actually provide a way to add the ':' in the timezone
    # offset. The only timezone offset code, %z, does not provide the
    # colon. Other implementations (GNU libc) offers the %:z option,
    # but that doesn't exist in Python and thus in Pandas.
    #
    # So if you're using Python to export timestamps with timezones,
    # you should probably use the `YYYY-MM-DD HH24:MI:SS` format and
    # express them in UTC.
    #
    # https://stackoverflow.com/questions/44836581/does-python-time-strftime-process-timezone-options-correctly-for-rfc-3339
    # https://stackoverflow.com/questions/28729212/pandas-save-date-in-iso-format
    #
    if hints.datetimeformat in ['YYYY-MM-DD HH24:MI:SS', 'YYYY-MM-DD HH:MI:SS']:
        pass
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformat', hints)
    quiet_remove(unhandled_hints, 'datetimeformat')

    if hints.datetimeformattz in ['YYYY-MM-DD HH:MI:SSOF',
                                  'YYYY-MM-DD HH24:MI:SSOF',
                                  'YYYY-MM-DD HH:MI:SS']:
        pass
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformattz', hints)
    quiet_remove(unhandled_hints, 'datetimeformattz')

    if hints.timeonlyformat in ['HH24:MI:SS', 'HH:MI:SS']:
        pass
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'timeonlyformat', hints)
    quiet_remove(unhandled_hints, 'timeonlyformat')

    # No options to change this.  Tested with unix newlines, dos
    # newlines and mac newlines and all were understood.:
    if hints.record_terminator in ['\n', '\r\n', '\r', None]:
        pass
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'record-terminator', hints)
    quiet_remove(unhandled_hints, 'record-terminator')

    # No way to flag compression, but tested uncompressed, with
    # gzip and works great.  .bz2 gives "400 Unsupported
    # compression type".  Not sure about .lzo, but pandas can't
    # handle it regardless, so doubt it's handled.
    if hints.compression is None or hints.compression == 'GZIP':
        pass
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'compression', hints)
    quiet_remove(unhandled_hints, 'compression')
