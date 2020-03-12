from ...utils import quiet_remove
from ...records.hints import cant_handle_hint
from ...records.load_plan import RecordsLoadPlan
from records_mover.records.types import RecordsHints
from ...records.records_format import DelimitedRecordsFormat
import logging
from typing import Union, Dict, Set, Tuple, Optional, TYPE_CHECKING
if TYPE_CHECKING:
    from typing import Literal
    # https://www.postgresql.org/docs/9.5/runtime-config-client.html#GUC-DATESTYLE
    #
    # DateStyle (string)
    #
    #  Sets the display format for date and time values, as well as
    #  the rules for interpreting ambiguous date input values. For
    #  historical reasons, this variable contains two independent
    #  components: the output format specification (ISO, Postgres,
    #  SQL, or German) and the input/output specification for
    #  year/month/day ordering (DMY, MDY, or YMD). These can be set
    #  separately or together. The keywords Euro and European are
    #  synonyms for DMY; the keywords US, NonEuro, and NonEuropean are
    #  synonyms for MDY. See Section 8.5 for more information. The
    #  built-in default is ISO, MDY, but initdb will initialize the
    #  configuration file with a setting that corresponds to the
    #  behavior of the chosen lc_time locale.
    DateInputStyle = Union[Literal["DMY"], Literal["MDY"]]
else:
    DateInputStyle = str


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
                                 postgres_options: PostgresCopyOptions) -> Optional[DateInputStyle]:
    date_input_style: Optional[DateInputStyle] = None

    # https://www.postgresql.org/docs/9.5/datatype-datetime.html#DATATYPE-DATETIME-INPUT

    # "Date and time input is accepted in almost any reasonable
    # format, including ISO 8601, SQL-compatible, traditional
    # POSTGRES, and others. For some formats, ordering of day, month,
    # and year in date input is ambiguous and there is support for
    # specifying the expected ordering of these fields. Set the
    # DateStyle parameter to MDY to select month-day-year
    # interpretation, DMY to select day-month-year interpretation, or
    # YMD to select year-month-day interpretation."


    # $ ./itest shell
    # $ db dockerized-postgres
    # postgres=# SHOW DateStyle;
    #  DateStyle
    # -----------
    #  ISO, MDY
    # (1 row)
    #
    # postgres=#

    # TODO: File GitHub issue documenting that we're assuming 'ISO,
    # MDY' as datestyle and describing a feature to switch DateStyle
    # appropriately to match the needed hint or to deal with
    # euro-style defaults.

    datetimeformattz = hints['datetimeformattz']

    # datetimeformattz: Valid values: "YYYY-MM-DD HH:MI:SSOF",
    # "YYYY-MM-DD HH:MI:SS", "YYYY-MM-DD HH24:MI:SSOF", "YYYY-MM-DD
    # HH24:MI:SSOF", "MM/DD/YY HH24:MI". See Redshift docs for more
    # information (note that HH: is equivalent to HH24: and that if
    # you don't provide an offset (OF), times are assumed to be in
    # UTC).

    # Default value is "YYYY-MM-DD HH:MI:SSOF".

    if datetimeformattz in ['YYYY-MM-DD HH:MI:SSOF',
                            'YYYY-MM-DD HH24:MI:SSOF']:
        #
        #  postgres=# select timestamptz '2020-01-01 23:01:01-10';
        #        timestamptz
        #  ------------------------
        #   2020-01-02 09:01:01+00
        #  (1 row)
        #
        #  postgres=#

        # Any DateStyle will do as this is unambiguous
        quiet_remove(unhandled_hints, 'datetimeformattz')
    elif datetimeformattz == 'YYYY-MM-DD HH:MI:SS':
        #
        #
        #  postgres=# select timestamptz '2020-01-01 23:01:01';
        #        timestamptz
        #  ------------------------
        #   2020-01-01 23:01:01+00
        #  (1 row)
        #
        #  postgres=#

        # Any DateStyle will do as this is unambiguous
        quiet_remove(unhandled_hints, 'datetimeformattz')
    elif datetimeformattz == "MM/DD/YY HH24:MI":
        # "MM/DD/YY HH24:MI"
        #
        #  postgres=# select timestamptz '01/02/2999 23:01';
        #        timestamptz
        #  ------------------------
        #   2999-01-02 23:01:00+00
        #  (1 row)
        #
        #  postgres=#
        if date_input_style not in (None, 'MDY'):
            cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformattz', hints)
        else:
            date_input_style = 'MDY'
            quiet_remove(unhandled_hints, 'datetimeformattz')
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformattz', hints)

    # datetimeformat: Valid values: "YYYY-MM-DD HH24:MI:SS",
    # "YYYY-MM-DD HH12:MI AM", "MM/DD/YY HH24:MI". See Redshift docs
    # for more information.

    datetimeformat = hints['datetimeformat']

    if datetimeformat == "YYYY-MM-DD HH24:MI:SS":
        #
        #  postgres=# select timestamp '2020-01-02 15:13:12';
        #        timestamp
        #  ---------------------
        #   2020-01-02 15:13:12
        #  (1 row)
        #
        #  postgres=#

        # Any DateStyle will do as this is unambiguous
        quiet_remove(unhandled_hints, 'datetimeformat')
    elif datetimeformat == "YYYY-MM-DD HH12:MI AM":
        # "YYYY-MM-DD HH12:MI AM"
        #
        #  postgres=# select timestamp '2020-01-02 1:13 PM';
        #        timestamp
        #  ---------------------
        #   2020-01-02 13:13:00
        #  (1 row)
        #
        #  postgres=#

        # Any DateStyle will do as this is unambiguous
        quiet_remove(unhandled_hints, 'datetimeformat')
    elif datetimeformat == "MM/DD/YY HH24:MI":
        #  postgres=# select timestamp '01/02/20 15:23';
        #        timestamp
        #  ---------------------
        #   2020-01-02 15:23:00
        #  (1 row)
        #
        #  postgres=#

        if date_input_style not in (None, 'MDY'):
            cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformat', hints)
        else:
            date_input_style = 'MDY'
            quiet_remove(unhandled_hints, 'datetimeformat')
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformat', hints)

    timeonlyformat = hints['timeonlyformat']

    # timeonlyformat: Valid values: "HH12:MI AM" (e.g., "1:00 PM"),
    # "HH24:MI:SS" (e.g., "13:00:00")

    if timeonlyformat == "HH12:MI AM":
        # "HH12:MI AM" (e.g., "1:00 PM"),
        #
        #  postgres=# select time '1:00 PM';
        #     time
        #  ----------
        #   13:00:00
        #  (1 row)
        #
        #  postgres=#

        # Supported!
        quiet_remove(unhandled_hints, 'timeonlyformat')
    elif timeonlyformat == "HH24:MI:SS":

        # "HH24:MI:SS" (e.g., "13:00:00")
        #
        #  postgres=# select time '13:00:00';
        #     time
        #  ----------
        #   13:00:00
        #  (1 row)
        #
        #  postgres=#

        # Supported!
        quiet_remove(unhandled_hints, 'timeonlyformat')
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformat', hints)

    # dateformat: Valid values: null, "YYYY-MM-DD", "MM-DD-YYYY", "DD-MM-YYYY", "MM/DD/YY".
    dateformat = hints['dateformat']

    if dateformat == "YYYY-MM-DD":
        #  postgres=# select date '1999-01-02';
        #      date
        #  ------------
        #   1999-01-02
        #  (1 row)
        #
        #  postgres=#
        # Any DateStyle will do as this is unambiguous
        quiet_remove(unhandled_hints, 'dateformat')
    elif dateformat == "MM-DD-YYYY":
        # "MM-DD-YYYY"
        #
        #  postgres=# select date '01-02-1999';
        #      date
        #  ------------
        #   1999-01-02
        #  (1 row)
        #
        #  postgres=#
        if date_input_style not in (None, 'MDY'):
            cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformattz', hints)
        else:
            date_input_style = 'MDY'
            quiet_remove(unhandled_hints, 'dateformat')
    elif dateformat == "DD-MM-YYYY":
        # "DD-MM-YYYY" - not supported by default, need to switch to
        # DMY:
        #
        #  postgres=# select date '02-01-1999';
        #      date
        #  ------------
        #   1999-02-01
        #  (1 row)
        #
        #  postgres=#
        if date_input_style not in (None, 'DMY'):
            cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformattz', hints)
        else:
            date_input_style = 'DMY'
            quiet_remove(unhandled_hints, 'dateformat')
    elif dateformat == "MM/DD/YY":
        # "MM/DD/YY".
        #
        #  postgres=# select date '01/02/99';
        #      date
        #  ------------
        #   1999-01-02
        #  (1 row)
        #
        #  postgres=#
        if date_input_style not in (None, 'MDY'):
            cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformattz', hints)
        else:
            date_input_style = 'MDY'
            quiet_remove(unhandled_hints, 'dateformat')
    elif dateformat is None:
        # null implies that that date format is unknown, and that the
        # implementation SHOULD generate using their default value and
        # parse permissively.

        # ...which is what Postgres does!
        quiet_remove(unhandled_hints, 'dateformat')
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'dateformat', hints)

    # ENCODING
    #
    #  Specifies that the file is encoded in the encoding_name. If
    #  this option is omitted, the current client encoding is
    #  used. See the Notes below for more details.

    encoding_hint: str = hints['encoding']  # type: ignore
    if encoding_hint in postgres_encoding_names:
        postgres_options['encoding'] = postgres_encoding_names[encoding_hint]
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

    return date_input_style


def postgres_copy_options_text(unhandled_hints: Set[str],
                               hints: RecordsHints,
                               fail_if_cant_handle_hint: bool,
                               postgres_options: PostgresCopyOptions) -> Optional[DateInputStyle]:
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

    return postgres_copy_options_common(unhandled_hints,
                                        hints,
                                        fail_if_cant_handle_hint,
                                        postgres_options)



def postgres_copy_options_csv(unhandled_hints: Set[str],
                              hints: RecordsHints,
                              fail_if_cant_handle_hint: bool,
                              postgres_options: PostgresCopyOptions) -> Optional[DateInputStyle]:
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

    return postgres_copy_options_common(unhandled_hints,
                                        hints,
                                        fail_if_cant_handle_hint,
                                        postgres_options)


def postgres_copy_options(unhandled_hints: Set[str],
                          load_plan: RecordsLoadPlan) -> Tuple[Optional[DateInputStyle],
                                                               PostgresCopyOptions]:
    fail_if_cant_handle_hint = load_plan.processing_instructions.fail_if_cant_handle_hint
    if not isinstance(load_plan.records_format, DelimitedRecordsFormat):
        raise NotImplementedError("Not currently able to import "
                                  f"{load_plan.records_format.format_type}")
    hints = load_plan.records_format.hints

    postgres_options: PostgresCopyOptions = {}
    if needs_csv_format(hints):
        date_input_style = postgres_copy_options_csv(unhandled_hints,
                                                     hints,
                                                     fail_if_cant_handle_hint,
                                                     postgres_options)
    else:
        date_input_style = postgres_copy_options_text(unhandled_hints,
                                                      hints,
                                                      fail_if_cant_handle_hint,
                                                      postgres_options)

    return (date_input_style, postgres_options)
