from ...utils import quiet_remove
from ...records.delimited import cant_handle_hint
from typing import Dict, Any, Set
from sqlalchemy_redshift.commands import Format
from records_mover.mover_types import _assert_never
from ...records.records_format import (
    DelimitedRecordsFormat, ParquetRecordsFormat, BaseRecordsFormat
)
from ...records.delimited.validated_records_hints import ValidatedRecordsHints

RedshiftUnloadOptions = Dict[str, Any]


def process_escape_chars(hints: ValidatedRecordsHints,
                         redshift_options: RedshiftUnloadOptions,
                         unhandled_hints: Set[str]) -> None:
    if hints.escape == '\\':
        redshift_options['escape'] = True
    elif hints.escape is not None:
        _assert_never(hints.escape)
    quiet_remove(unhandled_hints, 'escape')


def process_delimiter(hints: ValidatedRecordsHints,
                      unhandled_hints: Set[str],
                      redshift_options: RedshiftUnloadOptions
                      ) -> None:
    redshift_options['delimiter'] = hints.field_delimiter
    quiet_remove(unhandled_hints, 'field-delimiter')


def process_terminator(hints: ValidatedRecordsHints,
                       unhandled_hints: Set[str],
                       fail_if_cant_handle_hint: bool,
                       ) -> None:
    if hints.record_terminator != "\n":
        cant_handle_hint(fail_if_cant_handle_hint, 'record-terminator', hints)
    quiet_remove(unhandled_hints, 'record-terminator')


def process_quotechars(hints: ValidatedRecordsHints,
                       unhandled_hints: Set[str],
                       fail_if_cant_handle_hint: bool,
                       redshift_options: RedshiftUnloadOptions
                       ) -> None:
    if hints.quoting == 'all':
        if hints.doublequote is not False:
            cant_handle_hint(fail_if_cant_handle_hint, 'doublequote', hints)
        if hints.quotechar != '"':
            cant_handle_hint(fail_if_cant_handle_hint, 'quotechar', hints)
        redshift_options['add_quotes'] = True
    elif hints.quoting is None:
        redshift_options['add_quotes'] = False
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'quoting', hints)
    quiet_remove(unhandled_hints, 'quoting')
    quiet_remove(unhandled_hints, 'doublequote')
    quiet_remove(unhandled_hints, 'quotechar')


def process_compression(hints: ValidatedRecordsHints,
                        unhandled_hints: Set[str],
                        fail_if_cant_handle_hint: bool,
                        redshift_options: RedshiftUnloadOptions
                        ) -> None:
    if hints.compression == 'GZIP':
        redshift_options['gzip'] = True
    elif hints.compression is None:
        # good to go
        pass
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'compression', hints)
    quiet_remove(unhandled_hints, 'compression')


def process_encoding(hints: ValidatedRecordsHints,
                     unhandled_hints: Set[str],
                     fail_if_cant_handle_hint: bool
                     ) -> None:
    if hints.encoding != 'UTF8':
        cant_handle_hint(fail_if_cant_handle_hint, 'encoding', hints)
    quiet_remove(unhandled_hints, 'encoding')


def process_temporal_info(hints: ValidatedRecordsHints,
                          unhandled_hints: Set[str],
                          fail_if_cant_handle_hint: bool,
                          ) -> None:
    if hints.datetimeformattz not in ['YYYY-MM-DD HH:MI:SSOF',
                                      'YYYY-MM-DD HH24:MI:SSOF']:
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformattz', hints)
    quiet_remove(unhandled_hints, 'datetimeformattz')
    if hints.datetimeformat not in ['YYYY-MM-DD HH24:MI:SS',
                                    'YYYY-MM-DD HH:MI:SS']:
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformat', hints)
    quiet_remove(unhandled_hints, 'datetimeformat')
    if hints.dateformat != 'YYYY-MM-DD':
        cant_handle_hint(fail_if_cant_handle_hint, 'dateformat', hints)
    quiet_remove(unhandled_hints, 'dateformat')
    # Redshift doesn't have a time without date type, so there's
    # nothing that could be exported there.
    quiet_remove(unhandled_hints, 'timeonlyformat')


def process_header_row(hints: ValidatedRecordsHints,
                       unhandled_hints: Set[str],
                       fail_if_cant_handle_hint: bool,
                       ) -> None:
    if hints.header_row:
        # Redshift unload doesn't know how to add headers on an
        # unload.  Bleh.
        # https://stackoverflow.com/questions/24681214/unloading-from-redshift-to-s3-ith-headers
        cant_handle_hint(fail_if_cant_handle_hint, 'header-row', hints)
    else:
        quiet_remove(unhandled_hints, 'header-row')


# https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html
#
def redshift_unload_options(unhandled_hints: Set[str],
                            records_format: BaseRecordsFormat,
                            fail_if_cant_handle_hint: bool) -> RedshiftUnloadOptions:
    redshift_options: RedshiftUnloadOptions = {}

    if isinstance(records_format, ParquetRecordsFormat):
        redshift_options['format'] = Format.parquet
        return redshift_options
    if not isinstance(records_format, DelimitedRecordsFormat):
        raise NotImplementedError("Redshift export only supported via Parquet and "
                                  "delimited currently")

    hints = records_format.\
        validate(fail_if_cant_handle_hint=fail_if_cant_handle_hint)

    process_escape_chars(hints, redshift_options, unhandled_hints)
    process_delimiter(hints, unhandled_hints, redshift_options)
    process_terminator(hints, unhandled_hints, fail_if_cant_handle_hint)
    process_quotechars(hints, unhandled_hints, fail_if_cant_handle_hint, redshift_options)
    process_compression(hints, unhandled_hints, fail_if_cant_handle_hint, redshift_options)
    process_encoding(hints, unhandled_hints, fail_if_cant_handle_hint)
    process_temporal_info(hints, unhandled_hints, fail_if_cant_handle_hint)
    process_header_row(hints, unhandled_hints, fail_if_cant_handle_hint)

    return redshift_options
