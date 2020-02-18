from ...utils import quiet_remove
from ...records.hints import cant_handle_hint
from sqlalchemy_redshift.commands import Format
from typing import Dict, Optional, Set
from ...records import RecordsHints

RedshiftCopyOptions = Dict[str, Optional[object]]


def redshift_copy_options(unhandled_hints: Set[str],
                          hints: RecordsHints,
                          fail_if_cant_handle_hint: bool,
                          fail_if_row_invalid: bool,
                          max_failure_rows: Optional[int]) -> RedshiftCopyOptions:
    redshift_options: RedshiftCopyOptions = {}
    redshift_options['compression'] = hints['compression']
    quiet_remove(unhandled_hints, 'compression')
    if hints['dateformat'] is None:
        redshift_options['date_format'] = 'auto'
    else:
        redshift_options['date_format'] = hints['dateformat']
    quiet_remove(unhandled_hints, 'dateformat')
    if hints['encoding'] not in ['UTF8', 'UTF16', 'UTF16LE', 'UTF16BE']:
        cant_handle_hint(fail_if_cant_handle_hint, 'encoding', hints)
        redshift_options['encoding'] = 'UTF8'
    else:
        redshift_options['encoding'] = hints['encoding']
    quiet_remove(unhandled_hints, 'encoding')
    redshift_options['quote'] = hints['quotechar']
    quiet_remove(unhandled_hints, 'quotechar')
    if hints['quoting'] == 'minimal':
        if hints['escape'] is not None:
            cant_handle_hint(fail_if_cant_handle_hint, 'escape', hints)
        if hints['field-delimiter'] != ',':
            cant_handle_hint(fail_if_cant_handle_hint, 'field-delimiter', hints)
        if hints['doublequote'] is not True:
            cant_handle_hint(fail_if_cant_handle_hint, 'doublequote', hints)

        redshift_options['format'] = Format.csv
    else:
        redshift_options['delimiter'] = hints['field-delimiter']
        if hints['escape'] == '\\':
            redshift_options['escape'] = True
        elif hints['escape'] is None:
            redshift_options['escape'] = False
        else:
            cant_handle_hint(fail_if_cant_handle_hint, 'escape', hints)
            redshift_options['escape'] = False
        if hints['quoting'] == 'all':
            redshift_options['remove_quotes'] = True
            if hints['doublequote'] is not False:
                cant_handle_hint(fail_if_cant_handle_hint, 'doublequote', hints)

        elif hints['quoting'] is None:
            redshift_options['remove_quotes'] = False
        else:
            cant_handle_hint(fail_if_cant_handle_hint, 'quoting', hints)
    quiet_remove(unhandled_hints, 'quoting')
    quiet_remove(unhandled_hints, 'escape')
    quiet_remove(unhandled_hints, 'field-delimiter')
    quiet_remove(unhandled_hints, 'doublequote')
    if hints['datetimeformat'] is None:
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
        if hints['datetimeformat'] != hints['datetimeformattz']:
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
            redshift_options['time_format'] = hints['datetimeformat']
    quiet_remove(unhandled_hints, 'datetimeformat')
    quiet_remove(unhandled_hints, 'datetimeformattz')
    # Redshift doesn't support time-only fields, so these will
    # come in as strings regardless.
    quiet_remove(unhandled_hints, 'timeonlyformat')
    if max_failure_rows is not None:
        redshift_options['max_error'] = max_failure_rows
    elif fail_if_row_invalid:
        redshift_options['max_error'] = 0
    else:
        # max allowed value
        redshift_options['max_error'] = 100000
    if hints['record-terminator'] is not None and \
       hints['record-terminator'] != "\n":

        cant_handle_hint(fail_if_cant_handle_hint, 'record-terminator', hints)
    quiet_remove(unhandled_hints, 'record-terminator')

    if hints['header-row']:
        redshift_options['ignore_header'] = 1
    else:
        redshift_options['ignore_header'] = 0
    quiet_remove(unhandled_hints, 'header-row')

    return redshift_options
