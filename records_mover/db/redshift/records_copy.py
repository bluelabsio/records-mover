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
                 unhandled_hints: Set[str],
                 fail_if_row_invalid: bool,
                 max_failure_rows: Optional[int]):
        self.redshift_options: RedshiftCopyOptions = dict()
        self.hints: ValidatedRecordsHints = records_format.\
            validate(fail_if_cant_handle_hint=fail_if_cant_handle_hint)
        self.unhandled_hints = unhandled_hints
        self.fail_if_cant_handle_hint = fail_if_cant_handle_hint
        self.fail_if_row_invalid = fail_if_row_invalid
        self.max_failure_rows = max_failure_rows


def process_compression(redshift_copy_parameters: RedshiftCopyParamaters):
    compression_map = {
        'GZIP': Compression.gzip,
        'LZO': Compression.lzop,
        'BZIP': Compression.bzip2,
        None: None
    }
    compression = compression_map.get(redshift_copy_parameters.hints.compression)
    if compression is None and redshift_copy_parameters.hints.compression is not None:
        _assert_never(redshift_copy_parameters.hints.compression)  # type: ignore
    else:
        redshift_copy_parameters.redshift_options['compression'] = compression
    quiet_remove(redshift_copy_parameters.unhandled_hints, 'compression')


def process_dateformat(redshift_copy_parameters: RedshiftCopyParamaters):
    if redshift_copy_parameters.hints.dateformat is None:
        redshift_copy_parameters.redshift_options['date_format'] = 'auto'
    else:
        redshift_copy_parameters.redshift_options['date_format'] = (redshift_copy_parameters
                                                                    .hints
                                                                    .dateformat)
    quiet_remove(redshift_copy_parameters.unhandled_hints, 'dateformat')


def process_encoding(redshift_copy_parameters: RedshiftCopyParamaters):
    encoding_map = {
        'UTF8': Encoding.utf8,
        'UTF16': Encoding.utf16,
        'UTF16LE': Encoding.utf16le,
        'UTF16BE': Encoding.utf16be,
    }
    encoding = encoding_map.get(redshift_copy_parameters.hints.encoding)
    if not encoding:
        cant_handle_hint(redshift_copy_parameters.fail_if_cant_handle_hint,
                         'encoding', redshift_copy_parameters.hints)
        redshift_copy_parameters.redshift_options['encoding'] = Encoding(
            redshift_copy_parameters.hints.encoding)
    else:
        redshift_copy_parameters.redshift_options['encoding'] = encoding
    quiet_remove(redshift_copy_parameters.unhandled_hints, 'encoding')


def process_quotechar(redshift_copy_parameters: RedshiftCopyParamaters):
    redshift_copy_parameters.redshift_options['quote'] = redshift_copy_parameters.hints.quotechar
    quiet_remove(redshift_copy_parameters.unhandled_hints, 'quotechar')


def process_quoting(redshift_copy_parameters: RedshiftCopyParamaters):
    if redshift_copy_parameters.hints.quoting == 'minimal':
        if redshift_copy_parameters.hints.escape is not None:
            cant_handle_hint(redshift_copy_parameters.fail_if_cant_handle_hint,
                             'escape', redshift_copy_parameters.hints)
        if redshift_copy_parameters.hints.field_delimiter != ',':
            cant_handle_hint(redshift_copy_parameters.fail_if_cant_handle_hint,
                             'field-delimiter', redshift_copy_parameters.hints)
        if redshift_copy_parameters.hints.doublequote is not True:
            cant_handle_hint(redshift_copy_parameters.fail_if_cant_handle_hint,
                             'doublequote', redshift_copy_parameters.hints)

        redshift_copy_parameters.redshift_options['format'] = Format.csv
    else:
        redshift_copy_parameters.redshift_options['delimiter'] = (redshift_copy_parameters
                                                                  .hints
                                                                  .field_delimiter)
        if redshift_copy_parameters.hints.escape == '\\':
            redshift_copy_parameters.redshift_options['escape'] = True
        elif redshift_copy_parameters.hints.escape is None:
            redshift_copy_parameters.redshift_options['escape'] = False
        else:
            _assert_never(redshift_copy_parameters.hints.escape)
        if redshift_copy_parameters.hints.quoting == 'all':
            redshift_copy_parameters.redshift_options['remove_quotes'] = True
            if redshift_copy_parameters.hints.doublequote is not False:
                cant_handle_hint(redshift_copy_parameters.fail_if_cant_handle_hint,
                                 'doublequote', redshift_copy_parameters.hints)

        elif redshift_copy_parameters.hints.quoting is None:
            redshift_copy_parameters.redshift_options['remove_quotes'] = False
        else:
            cant_handle_hint(redshift_copy_parameters.fail_if_cant_handle_hint,
                             'quoting', redshift_copy_parameters.hints)
    quiet_remove(redshift_copy_parameters.unhandled_hints, 'quoting')
    quiet_remove(redshift_copy_parameters.unhandled_hints, 'escape')
    quiet_remove(redshift_copy_parameters.unhandled_hints, 'field-delimiter')
    quiet_remove(redshift_copy_parameters.unhandled_hints, 'doublequote')


def process_temporal_info(redshift_copy_parameters: RedshiftCopyParamaters):
    if redshift_copy_parameters.hints.datetimeformat is None:
        redshift_copy_parameters.redshift_options['time_format'] = 'auto'
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
        if redshift_copy_parameters.hints.datetimeformat != (redshift_copy_parameters
                                                             .hints
                                                             .datetimeformattz):
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
            redshift_copy_parameters.redshift_options['time_format'] = 'auto'
        else:
            redshift_copy_parameters.redshift_options['time_format'] = (redshift_copy_parameters
                                                                        .hints
                                                                        .datetimeformat)
    quiet_remove(redshift_copy_parameters.unhandled_hints, 'datetimeformat')
    quiet_remove(redshift_copy_parameters.unhandled_hints, 'datetimeformattz')
    # Redshift doesn't support time-only fields, so these will
    # come in as strings regardless.
    quiet_remove(redshift_copy_parameters.unhandled_hints, 'timeonlyformat')


def process_max_failure_rows(redshift_copy_parameters: RedshiftCopyParamaters):
    if redshift_copy_parameters.max_failure_rows is not None:
        redshift_copy_parameters.redshift_options['max_error'] = (redshift_copy_parameters
                                                                  .max_failure_rows)
    elif redshift_copy_parameters.fail_if_row_invalid:
        redshift_copy_parameters.redshift_options['max_error'] = 0
    else:
        # max allowed value
        redshift_copy_parameters.redshift_options['max_error'] = 100000


def process_records_terminator(redshift_copy_parameters: RedshiftCopyParamaters):
    if redshift_copy_parameters.hints.record_terminator is not None and \
       redshift_copy_parameters.hints.record_terminator != "\n":
        cant_handle_hint(redshift_copy_parameters.fail_if_cant_handle_hint,
                         'record-terminator', redshift_copy_parameters.hints)
    quiet_remove(redshift_copy_parameters.unhandled_hints, 'record-terminator')


def process_header_row(redshift_copy_parameters: RedshiftCopyParamaters):
    if redshift_copy_parameters.hints.header_row:
        redshift_copy_parameters.redshift_options['ignore_header'] = 1
    else:
        redshift_copy_parameters.redshift_options['ignore_header'] = 0
    quiet_remove(redshift_copy_parameters.unhandled_hints, 'header-row')


def redshift_copy_options(unhandled_hints: Set[str],
                          records_format: BaseRecordsFormat,
                          fail_if_cant_handle_hint: bool,
                          fail_if_row_invalid: bool,
                          max_failure_rows: Optional[int]) -> RedshiftCopyOptions:

    if isinstance(records_format, AvroRecordsFormat):
        redshift_options: RedshiftCopyOptions = dict()
        redshift_options['format'] = Format.avro
        return redshift_options
    elif isinstance(records_format, DelimitedRecordsFormat):
        redshift_copy_parameters = RedshiftCopyParamaters(records_format,
                                                          fail_if_cant_handle_hint,
                                                          unhandled_hints,
                                                          fail_if_row_invalid,
                                                          max_failure_rows)
    else:
        raise NotImplementedError(f"Teach me how to COPY to {records_format}")

    process_compression(redshift_copy_parameters)
    process_dateformat(redshift_copy_parameters)
    process_encoding(redshift_copy_parameters)
    process_quotechar(redshift_copy_parameters)
    process_quoting(redshift_copy_parameters)
    process_temporal_info(redshift_copy_parameters)
    process_max_failure_rows(redshift_copy_parameters)
    process_records_terminator(redshift_copy_parameters)
    process_header_row(redshift_copy_parameters)

    return redshift_copy_parameters.redshift_options
