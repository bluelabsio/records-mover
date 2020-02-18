import csv
from ...utils import quiet_remove
from ..hints import cant_handle_hint
from ..processing_instructions import ProcessingInstructions
from ..records_format import DelimitedRecordsFormat
import logging
from typing import Set, Dict


logger = logging.getLogger(__name__)


def pandas_to_csv_options(records_format: DelimitedRecordsFormat,
                          unhandled_hints: Set[str],
                          processing_instructions: ProcessingInstructions) -> Dict[str, object]:
    # https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_csv.html
    hints = records_format.hints

    fail_if_cant_handle_hint = processing_instructions.fail_if_cant_handle_hint

    pandas_options: Dict[str, object] = {}

    pandas_options['encoding'] = hints['encoding']
    quiet_remove(unhandled_hints, 'encoding')

    if hints['compression'] is None:
        # hints['compression']=None will output an uncompressed csv,
        # which is the pandas default.
        pass
    elif hints['compression'] == 'GZIP':
        pandas_options['compression'] = 'gzip'
    elif hints['compression'] == 'BZIP':
        pandas_options['compression'] = 'bz2'
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'compression', hints)
    quiet_remove(unhandled_hints, 'compression')

    if hints['quoting'] is None:
        pandas_options['quoting'] = csv.QUOTE_NONE
    elif hints['quoting'] == 'all':
        pandas_options['quoting'] = csv.QUOTE_ALL
    elif hints['quoting'] == 'minimal':
        pandas_options['quoting'] = csv.QUOTE_MINIMAL
    elif hints['quoting'] == 'nonnumeric':
        pandas_options['quoting'] = csv.QUOTE_NONNUMERIC
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'quoting', hints)
    quiet_remove(unhandled_hints, 'quoting')

    pandas_options['doublequote'] = hints['doublequote']
    quiet_remove(unhandled_hints, 'doublequote')
    pandas_options['quotechar'] = hints['quotechar']
    quiet_remove(unhandled_hints, 'quotechar')

    if hints['escape'] is None:
        pass
    else:
        pandas_options['escapechar'] = hints['escape']
    quiet_remove(unhandled_hints, 'escape')

    pandas_options['header'] = hints['header-row']
    quiet_remove(unhandled_hints, 'header-row')

    if hints['dateformat'] is None:
        if hints['datetimeformattz'] == hints['datetimeformat']:
            # BigQuery requires that timezone offsets have a colon;
            # Python (and thus Pandas) doesn't support adding the
            # colon with strftime.  However, we can specify things
            # without a timezone delimiter just fine.
            #
            # Unfortunately Python/Pandas will drop the timezone info
            # instead of converting the timestamp to UTC.  This
            # corrupts the time, as BigQuery assumes what it gets in
            # is UTC format.  Boo.
            #
            # $ python3
            # >>> import pytz
            # >>> us_eastern = pytz.timezone('US/Eastern')
            # >>> import datetime
            # >>> us_eastern.localize(datetime.datetime(2000, 1, 2, 12, 34, 56, 789012))
            #        .strftime('%Y-%m-%d %H:%M:%S.%f')
            # '2000-01-02 12:34:56.789012'
            # >>>
            #
            # https://app.asana.com/0/1128138765527694/1159958019131681
            pandas_options['date_format'] = '%Y-%m-%d %H:%M:%S.%f'
        else:
            pandas_options['date_format'] = '%Y-%m-%d %H:%M:%S.%f%z'
    elif hints['dateformat'] == 'YYYY-MM-DD':
        if hints['datetimeformattz'] == hints['datetimeformat']:
            pandas_options['date_format'] = '%Y-%m-%d %H:%M:%S.%f'
        else:
            pandas_options['date_format'] = '%Y-%m-%d %H:%M:%S.%f%z'
    elif hints['dateformat'] == 'MM-DD-YYYY':
        if hints['datetimeformattz'] == hints['datetimeformat']:
            pandas_options['date_format'] = '%m-%d-%Y %H:%M:%S.%f'
        else:
            pandas_options['date_format'] = '%m-%d-%Y %H:%M:%S.%f%z'
    elif hints['dateformat'] == 'DD-MM-YYYY':
        if hints['datetimeformattz'] == hints['datetimeformat']:
            pandas_options['date_format'] = '%d-%m-%Y %H:%M:%S.%f'
        else:
            pandas_options['date_format'] = '%d-%m-%Y %H:%M:%S.%f%z'
    elif hints['dateformat'] == 'MM/DD/YY':
        if hints['datetimeformattz'] == hints['datetimeformat']:
            pandas_options['date_format'] = '%m/%d/%y %H:%M:%S.%f'
        else:
            pandas_options['date_format'] = '%m/%d/%y %H:%M:%S.%f%z'
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'dateformat', hints)
    quiet_remove(unhandled_hints, 'dateformat')

    # pandas can't seem to export a date and time together :(
    #
    # might be nice someday to only emit the errors if the actual data
    # being moved is affected by whatever limitation...
    if (hints['datetimeformattz'] not in(f"{hints.get('dateformat', 'YYYY-MM-DD')} HH24:MI:SSOF",
                                         f"{hints.get('dateformat', 'YYYY-MM-DD')} HH:MI:SSOF",
                                         f"{hints.get('dateformat', 'YYYY-MM-DD')} HH24:MI:SS",
                                         f"{hints.get('dateformat', 'YYYY-MM-DD')} HH:MI:SS",
                                         f"{hints.get('dateformat', 'YYYY-MM-DD')} HH:MIOF",
                                         f"{hints.get('dateformat', 'YYYY-MM-DD')} HH:MI",
                                         f"{hints.get('dateformat', 'YYYY-MM-DD')} HH24:MIOF",
                                         f"{hints.get('dateformat', 'YYYY-MM-DD')} HH24:MI")):
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformattz', hints)
    quiet_remove(unhandled_hints, 'datetimeformattz')

    valid_datetimeformat = [
            f"{hints.get('dateformat', 'YYYY-MM-DD')} HH24:MI:SS",
            f"{hints.get('dateformat', 'YYYY-MM-DD')} HH:MI:SS",
            f"{hints.get('dateformat', 'YYYY-MM-DD')} HH24:MI",
            f"{hints.get('dateformat', 'YYYY-MM-DD')} HH:MI",
    ]
    if (hints['datetimeformat'] not in valid_datetimeformat):
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformat', hints)
    quiet_remove(unhandled_hints, 'datetimeformat')

    if hints['timeonlyformat'] != 'HH24:MI:SS':
        cant_handle_hint(fail_if_cant_handle_hint, 'timeonlyformat', hints)
    quiet_remove(unhandled_hints, 'timeonlyformat')

    pandas_options['sep'] = hints['field-delimiter']
    quiet_remove(unhandled_hints, 'field-delimiter')

    pandas_options['line_terminator'] = hints['record-terminator']
    quiet_remove(unhandled_hints, 'record-terminator')

    return pandas_options
