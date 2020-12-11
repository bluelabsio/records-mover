import csv
from ...utils import quiet_remove
from ..delimited import cant_handle_hint
from ..processing_instructions import ProcessingInstructions
from ..records_format import DelimitedRecordsFormat
from records_mover.mover_types import _assert_never
import logging
from typing import Set, Dict


logger = logging.getLogger(__name__)


def pandas_to_csv_options(records_format: DelimitedRecordsFormat,
                          unhandled_hints: Set[str],
                          processing_instructions: ProcessingInstructions) -> Dict[str, object]:
    # https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_csv.html
    hints = records_format.\
        validate(fail_if_cant_handle_hint=processing_instructions.fail_if_cant_handle_hint)

    fail_if_cant_handle_hint = processing_instructions.fail_if_cant_handle_hint

    pandas_options: Dict[str, object] = {}

    pandas_options['encoding'] = hints.encoding
    quiet_remove(unhandled_hints, 'encoding')

    if hints.compression is None:
        # hints['compression']=None will output an uncompressed csv,
        # which is the pandas default.
        pass
    elif hints.compression == 'GZIP':
        pandas_options['compression'] = 'gzip'
    elif hints.compression == 'BZIP':
        pandas_options['compression'] = 'bz2'
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'compression', hints)
    quiet_remove(unhandled_hints, 'compression')

    if hints.quoting is None:
        pandas_options['quoting'] = csv.QUOTE_NONE
    elif hints.quoting == 'all':
        pandas_options['quoting'] = csv.QUOTE_ALL
    elif hints.quoting == 'minimal':
        pandas_options['quoting'] = csv.QUOTE_MINIMAL
    elif hints.quoting == 'nonnumeric':
        pandas_options['quoting'] = csv.QUOTE_NONNUMERIC
    else:
        _assert_never(hints.quoting)
    quiet_remove(unhandled_hints, 'quoting')

    pandas_options['doublequote'] = hints.doublequote
    quiet_remove(unhandled_hints, 'doublequote')
    pandas_options['quotechar'] = hints.quotechar
    quiet_remove(unhandled_hints, 'quotechar')

    if hints.escape is None:
        pass
    else:
        pandas_options['escapechar'] = hints.escape
    quiet_remove(unhandled_hints, 'escape')

    pandas_options['header'] = hints.header_row
    quiet_remove(unhandled_hints, 'header-row')

    # Note the limitation on Pandas export with BigQuery around
    # datetimeformattz:
    #
    # https://github.com/bluelabsio/records-mover/issues/95

    # Pandas only gives us one parameter to set for formatting of its
    # Timestamp values, so we need the datetimeformat and
    # datetimeformattz hints to be nearly identical, modulo the
    # timezone at the end which will only appear if it is set in the
    # source data anyway:

    canonical_datetimeformattz = hints.datetimeformattz.replace('HH24', 'HH')
    canonical_datetimeformat = hints.datetimeformat.replace('HH24', 'HH')
    equivalent_with_timezone = f"{canonical_datetimeformat}OF"

    if (canonical_datetimeformattz not in
       [canonical_datetimeformat, equivalent_with_timezone]):
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformat', hints)

    if 'AM' in hints.datetimeformattz:
        hour_specifier = '%I'
    else:
        hour_specifier = '%H'

    pandas_options['date_format'] = hints.datetimeformattz\
        .replace('YYYY', '%Y')\
        .replace('YY', '%y')\
        .replace('MM', '%m')\
        .replace('DD', '%d')\
        .replace('HH24', '%H')\
        .replace('HH12', '%I')\
        .replace('HH', hour_specifier)\
        .replace('MI', '%M')\
        .replace('SS', '%S.%f')\
        .replace('OF', '%z')\
        .replace('AM', '%p')
    quiet_remove(unhandled_hints, 'datetimeformat')
    quiet_remove(unhandled_hints, 'datetimeformattz')

    # timeonlyformat and dateformat are handled in prep_for_csv.py and
    # raw times and dates never appear in dataframes passed a
    # .to_csv() call.
    quiet_remove(unhandled_hints, 'timeonlyformat')
    quiet_remove(unhandled_hints, 'dateformat')

    pandas_options['sep'] = hints.field_delimiter
    quiet_remove(unhandled_hints, 'field-delimiter')

    pandas_options['line_terminator'] = hints.record_terminator
    quiet_remove(unhandled_hints, 'record-terminator')

    return pandas_options
