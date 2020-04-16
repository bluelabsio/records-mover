from records_mover.utils import quiet_remove
from records_mover.records.hints import cant_handle_hint
from records_mover.records.types import RecordsHints
from typing import Set
from .common import postgres_copy_options_common
from .types import PostgresCopyOptions


def postgres_copy_options_csv(unhandled_hints: Set[str],
                              hints: RecordsHints,
                              fail_if_cant_handle_hint: bool) ->\
        PostgresCopyOptions:
    postgres_options: PostgresCopyOptions = {}
    # FORMAT
    #
    #  Selects the data format to be read or written: text, csv (Comma
    #  Separated Values), or binary. The default is text.
    #
    postgres_options['format'] = 'csv'

    # NULL
    #
    #  Specifies the string that represents a null value. The default
    #  is \N (backslash-N) in text format, and an unquoted empty
    #  string in CSV format. You might prefer an empty string even in
    #  text format for cases where you don't want to distinguish nulls
    #  from empty strings. This option is not allowed when using
    #  binary format.
    #
    #  Note: When using COPY FROM, any data item that matches this
    #  string will be stored as a null value, so you should make sure
    #  that you use the same string as you used with COPY TO.
    #

    # QUOTE
    #
    #  Specifies the quoting character to be used when a data value is
    #  quoted. The default is double-quote. This must be a single
    #  one-byte character. This option is allowed only when using CSV
    #  format.
    #

    postgres_options['quote'] = hints['quotechar']
    quiet_remove(unhandled_hints, 'quotechar')

    # ESCAPE
    #
    #  Specifies the character that should appear before a data
    #  character that matches the QUOTE value. The default is the same
    #  as the QUOTE value (so that the quoting character is doubled if
    #  it appears in the data). This must be a single one-byte
    #  character. This option is allowed only when using CSV format.
    #

    if not hints['doublequote']:
        cant_handle_hint(fail_if_cant_handle_hint, 'doublequote', hints)
    else:
        quiet_remove(unhandled_hints, 'doublequote')

    if hints['escape'] is not None:
        cant_handle_hint(fail_if_cant_handle_hint, 'escape', hints)
    else:
        quiet_remove(unhandled_hints, 'escape')

    # FORCE_QUOTE
    #
    #  Forces quoting to be used for all non-NULL values in each
    #  specified column. NULL output is never quoted. If * is
    #  specified, non-NULL values will be quoted in all columns. This
    #  option is allowed only in COPY TO, and only when using CSV
    #  format.
    #

    if hints['quoting'] != 'minimal':
        cant_handle_hint(fail_if_cant_handle_hint, 'quoting', hints)
    else:
        quiet_remove(unhandled_hints, 'quoting')

    # As of the 9.2 release (documentation as of 2019-03-12), there's
    # no statement in the docs on what newline formats are accepted in
    # "CSV" mode:
    #
    # https://www.postgresql.org/docs/9.2/sql-copy.html#AEN67247
    #
    # So let's test and find out!
    #
    # This test file is in UNIX newline mode:
    #
    # $ file tests/integration/resources/delimited-bigquery-with-header.csv
    # tests/integration/resources/delimited-bigquery-with-header.csv: ASCII text
    # $
    # It loads fine with:
    # $ mvrec file2table --source.variant bigquery
    # --source.no_compression tests/integration/resources/delimited-bigquery-with-header.csv
    # --target.existing_table drop_and_recreate dockerized-postgres public bigqueryformat
    # $ unix2mac -n tests/integration/resources/delimited-bigquery-with-header.csv
    # tests/integration/resources/delimited-bigquery-with-header-mac.csv
    # $ mvrec file2table --source.variant bigquery --source.no_compression
    # tests/integration/resources/delimited-bigquery-with-header-mac.csv
    # dockerized-postgres public bigqueryformat # loads fine
    # $ unix2dos -n tests/integration/resources/delimited-bigquery-with-header.csv
    # tests/integration/resources/delimited-bigquery-with-header-dos.csv
    # $ mvrec file2table --source.variant bigquery --source.no_compression
    # tests/integration/resources/delimited-bigquery-with-header-dos.csv
    # --target.existing_table drop_and_recreate
    # dockerized-postgres public bigqueryformat # loads fine
    if hints['record-terminator'] in ("\n", "\r\n", "\r", None):
        quiet_remove(unhandled_hints, 'record-terminator')
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'records-terminator', hints)

    if hints['compression'] is not None:
        cant_handle_hint(fail_if_cant_handle_hint, 'compression', hints)
    else:
        quiet_remove(unhandled_hints, 'compression')

    return postgres_copy_options_common(unhandled_hints,
                                        hints,
                                        fail_if_cant_handle_hint,
                                        postgres_options)
