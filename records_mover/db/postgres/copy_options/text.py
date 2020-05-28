from records_mover.utils import quiet_remove
from records_mover.records.delimited import cant_handle_hint, ValidatedRecordsHints
from typing import Set
from .mode import CopyOptionsMode
from .types import PostgresCopyOptions, CopyOptionsModeType, _assert_never
from .common import postgres_copy_options_common


def postgres_copy_options_text(unhandled_hints: Set[str],
                               hints: ValidatedRecordsHints,
                               fail_if_cant_handle_hint: bool,
                               mode: CopyOptionsModeType) ->\
        PostgresCopyOptions:
    postgres_options: PostgresCopyOptions = {}

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
    if hints.escape is None:
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

    if hints.doublequote:
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

    if hints.quoting is not None:
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

    if mode is CopyOptionsMode.LOADING:
        if hints.record_terminator in ["\n", "\r", "\r\n"]:
            quiet_remove(unhandled_hints, 'record-terminator')
        else:
            cant_handle_hint(fail_if_cant_handle_hint, 'record-terminator', hints)
    elif mode is CopyOptionsMode.UNLOADING:
        # No control for this is given - exports appear with unix
        # newlines.
        if hints.record_terminator == "\n":
            quiet_remove(unhandled_hints, 'record-terminator')
        else:
            cant_handle_hint(fail_if_cant_handle_hint, 'record-terminator', hints)
    else:
        _assert_never(mode)

    if hints.compression is not None:
        cant_handle_hint(fail_if_cant_handle_hint, 'compression', hints)
    else:
        quiet_remove(unhandled_hints, 'compression')

    return postgres_copy_options_common(unhandled_hints,
                                        hints,
                                        fail_if_cant_handle_hint,
                                        postgres_options)
