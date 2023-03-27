from ...utils import quiet_remove
from ...records.delimited import cant_handle_hint
from records_mover.records.records_format import (
    BaseRecordsFormat, DelimitedRecordsFormat, AvroRecordsFormat
)
from records_mover.records.delimited import ValidatedRecordsHints
from records_mover.mover_types import _assert_never
from sqlalchemy_redshift.commands import Format, Encoding, Compression
from typing import Dict, Optional, Set

RedshiftCopyOptions = Dict[str, Optional[object]]


class RedshiftCopyParamaters:
    def __init__(self,
                 records_format: DelimitedRecordsFormat,
                 fail_if_cant_handle_hint: bool,
                 unhandled_hints: Set[str]):
        self.redshift_options: RedshiftCopyOptions = {}
        self.hints = records_format.\
            validate(fail_if_cant_handle_hint=fail_if_cant_handle_hint)
        self.unhandled_hints = unhandled_hints
        self.fail_if_cant_handle_hint = fail_if_cant_handle_hint


def process_compression(redshift_options: RedshiftCopyOptions,
                        hints: ValidatedRecordsHints,
                        unhandled_hints: Set[str]):
    compression_map = {
        'GZIP': Compression.gzip,
        'LZO': Compression.lzop,
        'BZIP': Compression.bzip2,
        None: None
    }
    hints_compression: Optional[str] = hints.compression
    compression = compression_map.get(hints_compression)
    if compression is None and hints_compression is not None:
        _assert_never(hints_compression)
    else:
        redshift_options['compression'] = compression
    quiet_remove(unhandled_hints, 'compression')


def process_dateformat(redshift_options: RedshiftCopyOptions,
                       hints: ValidatedRecordsHints,
                       unhandled_hints: Set[str]):
    if hints.dateformat is None:
        redshift_options['date_format'] = 'auto'
    else:
        redshift_options['date_format'] = hints.dateformat
    quiet_remove(unhandled_hints, 'dateformat')


def process_encoding(redshift_options: RedshiftCopyOptions,
                     hints: ValidatedRecordsHints,
                     unhandled_hints: Set[str],
                     fail_if_cant_handle_hint: bool):
    encoding_map = {
        'UTF8': Encoding.utf8,
        'UTF16': Encoding.utf16,
        'UTF16LE': Encoding.utf16le,
        'UTF16BE': Encoding.utf16be,
    }
    encoding = encoding_map.get(hints.encoding)
    if not encoding:
        cant_handle_hint(fail_if_cant_handle_hint, 'encoding', hints)
        redshift_options['encoding'] = Encoding(hints.encoding)
    else:
        redshift_options['encoding'] = encoding
    quiet_remove(unhandled_hints, 'encoding')


def process_quotechar(redshift_options: RedshiftCopyOptions,
                      hints: ValidatedRecordsHints,
                      unhandled_hints: Set[str]):
    redshift_options['quote'] = hints.quotechar
    quiet_remove(unhandled_hints, 'quotechar')


def process_quoting(redshift_options: RedshiftCopyOptions,
                    hints: ValidatedRecordsHints,
                    unhandled_hints: Set[str],
                    fail_if_cant_handle_hint: bool):
    if hints.quoting == 'minimal':
        if hints.escape is not None:
            cant_handle_hint(fail_if_cant_handle_hint, 'escape', hints)
        if hints.field_delimiter != ',':
            cant_handle_hint(fail_if_cant_handle_hint, 'field-delimiter', hints)
        if hints.doublequote is not True:
            cant_handle_hint(fail_if_cant_handle_hint, 'doublequote', hints)

        redshift_options['format'] = Format.csv
    else:
        redshift_options['delimiter'] = hints.field_delimiter
        if hints.escape == '\\':
            redshift_options['escape'] = True
        elif hints.escape is None:
            redshift_options['escape'] = False
        else:
            _assert_never(hints.escape)
        if hints.quoting == 'all':
            redshift_options['remove_quotes'] = True
            if hints.doublequote is not False:
                cant_handle_hint(fail_if_cant_handle_hint, 'doublequote', hints)

        elif hints.quoting is None:
            redshift_options['remove_quotes'] = False
        else:
            cant_handle_hint(fail_if_cant_handle_hint, 'quoting', hints)
    quiet_remove(unhandled_hints, 'quoting')
    quiet_remove(unhandled_hints, 'escape')
    quiet_remove(unhandled_hints, 'field-delimiter')
    quiet_remove(unhandled_hints, 'doublequote')


def process_temporal_info(redshift_options: RedshiftCopyOptions,
                          hints: ValidatedRecordsHints,
                          unhandled_hints: Set[str]):
    if hints.datetimeformat is None:
        redshift_options['time_format'] = 'auto'
    else:
        # After testing, Redshift's date/time parsing doesn't actually
        # support timezone parsing if you give it configuration - as
        # documented below, it doesn't accept a time zone as part of
        # the format string, and in experimentation, it silently drops
        # the offset when data into a timestamptz field if you specify
        # one directly.
        #
        # Its automatic parser seems to be smarter, though, and is
        # likely to handle a variety of formats:
        #
        # https://docs.aws.amazon.com/redshift/latest/dg/automatic-recognition.html
        if hints.datetimeformat != hints.datetimeformattz:
            # The Redshift auto parser seems to take a good handling
            # at our various supported formats, so let's give it a
            # shot if we're not able to specify a specific format due
            # to the Redshift timestamptz limitation:
            #
            # analytics=> create table formattest (test char(32));
            # CREATE TABLE
            # analytics=> insert into formattest values('2018-01-01 12:34:56');
            # INSERT 0 1
            # analytics=> insert into formattest values('01/02/18 15:34');
            # INSERT 0 1
            # analytics=> insert into formattest values('2018-01-02 15:34:12');
            # INSERT 0 1
            # analytics=> insert into formattest values('2018-01-02 10:34 PM');
            # INSERT 0 1
            # analytics=> select test, cast(test as timestamp) as timestamp,
            #             cast(test as date) as date from formattest;
            #
            #                test               |      timestamp      |    date
            # ----------------------------------+---------------------+------------
            #  2018-01-01 12:34:56              | 2018-01-01 12:34:56 | 2018-01-01
            #  01/02/18 15:34                   | 2018-01-02 15:34:00 | 2018-01-02
            #  2018-01-02 15:34:12              | 2018-01-02 15:34:12 | 2018-01-02
            #  2018-01-02 10:34 PM              | 2018-01-02 22:34:00 | 2018-01-02
            # (4 rows)
            #
            # analytics=>
            redshift_options['time_format'] = 'auto'
        else:
            redshift_options['time_format'] = hints.datetimeformat
    quiet_remove(unhandled_hints, 'datetimeformat')
    quiet_remove(unhandled_hints, 'datetimeformattz')
    # Redshift doesn't support time-only fields, so these will
    # come in as strings regardless.
    quiet_remove(unhandled_hints, 'timeonlyformat')


def process_max_failure_rows(redshift_options: RedshiftCopyOptions,
                             max_failure_rows: Optional[int],
                             fail_if_row_invalid: bool):
    if max_failure_rows is not None:
        redshift_options['max_error'] = max_failure_rows
    elif fail_if_row_invalid:
        redshift_options['max_error'] = 0
    else:
        # max allowed value
        redshift_options['max_error'] = 100000


def process_records_terminator(hints: ValidatedRecordsHints,
                               unhandled_hints: Set[str],
                               fail_if_cant_handle_hint: bool):
    if hints.record_terminator is not None and \
       hints.record_terminator != "\n":
        cant_handle_hint(fail_if_cant_handle_hint, 'record-terminator', hints)
    quiet_remove(unhandled_hints, 'record-terminator')


def process_header_row(hints: ValidatedRecordsHints,
                       redshift_options: RedshiftCopyOptions,
                       unhandled_hints: Set[str]):
    if hints.header_row:
        redshift_options['ignore_header'] = 1
    else:
        redshift_options['ignore_header'] = 0
    quiet_remove(unhandled_hints, 'header-row')


def redshift_copy_options(unhandled_hints: Set[str],
                          records_format: BaseRecordsFormat,
                          fail_if_cant_handle_hint: bool,
                          fail_if_row_invalid: bool,
                          max_failure_rows: Optional[int]) -> RedshiftCopyOptions:
    redshift_options: RedshiftCopyOptions = dict()

    if isinstance(records_format, AvroRecordsFormat):
        redshift_options['format'] = Format.avro
        return redshift_options

    if not isinstance(records_format, DelimitedRecordsFormat):
        raise NotImplementedError(f"Teach me how to COPY to {records_format}")

    hints: ValidatedRecordsHints = records_format.\
        validate(fail_if_cant_handle_hint=fail_if_cant_handle_hint)

    process_compression(redshift_options, hints, unhandled_hints)
    process_dateformat(redshift_options, hints, unhandled_hints)
    process_encoding(redshift_options, hints, unhandled_hints, fail_if_cant_handle_hint)
    process_quotechar(redshift_options, hints, unhandled_hints)
    process_quoting(redshift_options, hints, unhandled_hints, fail_if_cant_handle_hint)
    process_temporal_info(redshift_options, hints, unhandled_hints)
    process_max_failure_rows(redshift_options, max_failure_rows, fail_if_row_invalid)
    process_records_terminator(hints, unhandled_hints, fail_if_cant_handle_hint)
    process_header_row(hints, redshift_options, unhandled_hints)

    return redshift_options
