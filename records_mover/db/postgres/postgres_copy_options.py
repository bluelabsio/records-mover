from ...utils import quiet_remove
from ...records.hints import cant_handle_hint
from typing import Dict, Set
from ...records.load_plan import RecordsLoadPlan
from records_mover.records.types import RecordsHints
from ...records.records_format import DelimitedRecordsFormat
import logging


logger = logging.getLogger(__name__)


PostgresCopyOptions = Dict[str, object]

# https://www.postgresql.org/docs/9.2/sql-copy.html

# https://www.postgresql.org/docs/9.3/multibyte.html
postgres_encoding_names = {
    'UTF8': 'UTF8',
    # UTF-16 not supported
    # https://www.postgresql.org/message-id/20051028061108.GB30916%40london087.server4you.de
    'LATIN1': 'LATIN1',
    'CP1252': 'WIN1252',
}


def needs_csv_format(hints: RecordsHints) -> bool:
    # This format option is used for importing and exporting the Comma
    # Separated Value (CSV) file format used by many other programs,
    # such as spreadsheets. Instead of the escaping rules used by
    # PostgreSQL's standard text format, it produces and recognizes
    # the common CSV escaping mechanism.

    # The values in each record are separated by the DELIMITER
    # character. If the value contains the delimiter character, the
    # QUOTE character, the NULL string, a carriage return, or line
    # feed character, then the whole value is prefixed and suffixed by
    # the QUOTE character, and any occurrence within the value of a
    # QUOTE character or the ESCAPE character is preceded by the
    # escape character. You can also use FORCE_QUOTE to force quotes
    # when outputting non-NULL values in specific columns.
    if hints['header-row'] or (hints['quoting'] is not None):
        return True

    return False


def postgres_copy_options_common(unhandled_hints: Set[str],
                                 hints: RecordsHints,
                                 fail_if_cant_handle_hint: bool,
                                 postgres_options: PostgresCopyOptions) -> None:

    # TODO: Get this to work on read and write and test what we
    # produce vs what we can accept
    quiet_remove(unhandled_hints, 'datetimeformattz')

    # TODO: Get this to work on read and write and test what we
    # produce vs what we can accept
    quiet_remove(unhandled_hints, 'datetimeformat')

    # TODO: Get this to work on read and write and test what we
    # produce vs what we can accept
    quiet_remove(unhandled_hints, 'timeonlyformat')

    # TODO: Get this to work on read and write and test what we
    # produce vs what we can accept
    quiet_remove(unhandled_hints, 'dateformat')

    # ENCODING
    #
    #  Specifies that the file is encoded in the encoding_name. If
    #  this option is omitted, the current client encoding is
    #  used. See the Notes below for more details.

    # TODO: Dedupe common params
    if hints['encoding'] in postgres_encoding_names:
        postgres_options['encoding'] = postgres_encoding_names[hints['encoding']]
        quiet_remove(unhandled_hints, 'encoding')
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'encoding', hints)

    # FORCE_NOT_NULL
    #
    #  Do not match the specified columns' values against the null
    #  string. In the default case where the null string is empty,
    #  this means that empty values will be read as zero-length
    #  strings rather than nulls, even when they are not quoted. This
    #  option is allowed only in COPY FROM, and only when using CSV
    #  format.
    #
    # HEADER
    #
    #  Specifies that the file contains a header line with the names
    #  of each column in the file. On output, the first line contains
    #  the column names from the table, and on input, the first line
    #  is ignored. This option is allowed only when using CSV format.
    #
    quiet_remove(unhandled_hints, 'header-row')
    postgres_options['header'] = hints['header-row']

    # OIDS
    #
    #  Specifies copying the OID for each row. (An error is raised if
    #  OIDS is specified for a table that does not have OIDs, or in
    #  the case of copying a query.)
    #


    # DELIMITER
    #
    #  Specifies the character that separates columns within each row
    #  (line) of the file. The default is a tab character in text
    #  format, a comma in CSV format. This must be a single one-byte
    #  character. This option is not allowed when using binary format.
    #
    postgres_options['delimiter'] = hints['field-delimiter']
    quiet_remove(unhandled_hints, 'field-delimiter')


def postgres_copy_options_text(unhandled_hints: Set[str],
                               hints: RecordsHints,
                               fail_if_cant_handle_hint: bool,
                               postgres_options: PostgresCopyOptions) -> None:
    # FORMAT
    #
    #  Selects the data format to be read or written: text, csv (Comma
    #  Separated Values), or binary. The default is text.
    #
    postgres_options['format'] = 'text'

    # Backslash characters (\) can be used in the COPY data to quote
    # data characters that might otherwise be taken as row or column
    # delimiters. In particular, the following characters must be
    # preceded by a backslash if they appear as part of a column
    # value: backslash itself, newline, carriage return, and the
    # current delimiter character.
    if hints['escape'] is None:
        cant_handle_hint(fail_if_cant_handle_hint, 'escape', hints)
    else:
        quiet_remove(unhandled_hints, 'escape')

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

    quiet_remove(unhandled_hints, 'quotechar')

    # ESCAPE
    #
    #  Specifies the character that should appear before a data
    #  character that matches the QUOTE value. The default is the same
    #  as the QUOTE value (so that the quoting character is doubled if
    #  it appears in the data). This must be a single one-byte
    #  character. This option is allowed only when using CSV format.
    #

    if hints['doublequote']:
        cant_handle_hint(fail_if_cant_handle_hint, 'doublequote', hints)
    else:
        quiet_remove(unhandled_hints, 'doublequote')

    # FORCE_QUOTE
    #
    #  Forces quoting to be used for all non-NULL values in each
    #  specified column. NULL output is never quoted. If * is
    #  specified, non-NULL values will be quoted in all columns. This
    #  option is allowed only in COPY TO, and only when using CSV
    #  format.
    #

    if hints['quoting'] is not None:
        cant_handle_hint(fail_if_cant_handle_hint, 'quoting', hints)
    else:
        quiet_remove(unhandled_hints, 'quoting')

    # FORCE_NOT_NULL
    #
    #  Do not match the specified columns' values against the null
    #  string. In the default case where the null string is empty,
    #  this means that empty values will be read as zero-length
    #  strings rather than nulls, even when they are not quoted. This
    #  option is allowed only in COPY FROM, and only when using CSV
    #  format.
    #

    # COPY TO will terminate each row with a Unix-style newline
    # ("\n"). Servers running on Microsoft Windows instead output
    # carriage return/newline ("\r\n"), but only for COPY to a server
    # file; for consistency across platforms, COPY TO STDOUT always
    # sends "\n" regardless of server platform. COPY FROM can handle
    # lines ending with newlines, carriage returns, or carriage
    # return/newlines. To reduce the risk of error due to
    # un-backslashed newlines or carriage returns that were meant as
    # data, COPY FROM will complain if the line endings in the input
    # are not all alike.

    if hints['record-terminator'] in ["\n", "\r", "\r\n"]:
        quiet_remove(unhandled_hints, 'record-terminator')
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'record-terminator', hints)

    if hints['compression'] is not None:
        cant_handle_hint(fail_if_cant_handle_hint, 'compression', hints)
    else:
        quiet_remove(unhandled_hints, 'compression')

    postgres_copy_options_common(unhandled_hints,
                                 hints,
                                 fail_if_cant_handle_hint,
                                 postgres_options)



def postgres_copy_options_csv(unhandled_hints: Set[str],
                              hints: RecordsHints,
                              fail_if_cant_handle_hint: bool,
                              postgres_options: PostgresCopyOptions) -> None:
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

    # TODO: Get this to work on read and write and test what we
    # produce vs what we can accept
    quiet_remove(unhandled_hints, 'record-terminator')

    if hints['compression'] is not None:
        cant_handle_hint(fail_if_cant_handle_hint, 'compression', hints)
    else:
        quiet_remove(unhandled_hints, 'compression')

    postgres_copy_options_common(unhandled_hints,
                                 hints,
                                 fail_if_cant_handle_hint,
                                 postgres_options)


def postgres_copy_options(unhandled_hints: Set[str],
                          load_plan: RecordsLoadPlan) -> PostgresCopyOptions:
    fail_if_cant_handle_hint = load_plan.processing_instructions.fail_if_cant_handle_hint
    if not isinstance(load_plan.records_format, DelimitedRecordsFormat):
        raise NotImplementedError("Not currently able to import "
                                  f"{load_plan.records_format.format_type}")
    hints = load_plan.records_format.hints

    postgres_options: PostgresCopyOptions = {}
    if needs_csv_format(hints):
        postgres_copy_options_csv(unhandled_hints,
                                  hints,
                                  fail_if_cant_handle_hint,
                                  postgres_options)
    else:
        postgres_copy_options_text(unhandled_hints,
                                   hints,
                                   fail_if_cant_handle_hint,
                                   postgres_options)

    return postgres_options
