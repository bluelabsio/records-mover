from records_mover.utils import quiet_remove
from records_mover.records.hints import cant_handle_hint
from records_mover.records.types import RecordsHints
from typing import Set, Optional, Tuple
from .date_input_style import DateInputStyle, determine_date_input_style
from .types import PostgresCopyOptions


# https://www.postgresql.org/docs/9.3/multibyte.html
postgres_encoding_names = {
    'UTF8': 'UTF8',
    # UTF-16 not supported
    # https://www.postgresql.org/message-id/20051028061108.GB30916%40london087.server4you.de
    'LATIN1': 'LATIN1',
    'CP1252': 'WIN1252',
}


def postgres_copy_options_common(unhandled_hints: Set[str],
                                 hints: RecordsHints,
                                 fail_if_cant_handle_hint: bool,
                                 original_postgres_options: PostgresCopyOptions) ->\
        Tuple[Optional[DateInputStyle], PostgresCopyOptions]:
    postgres_options = original_postgres_options.copy()
    date_input_style: Optional[DateInputStyle] =\
        determine_date_input_style(unhandled_hints,
                                   hints,
                                   fail_if_cant_handle_hint)

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

    return (date_input_style, postgres_options)
