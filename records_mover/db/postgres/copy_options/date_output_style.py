from records_mover.utils import quiet_remove
from records_mover.records.delimited import cant_handle_hint, ValidatedRecordsHints
from typing import Set, Tuple, Optional
from .types import DateOrderStyle, DateOutputStyle


def determine_date_output_style(unhandled_hints: Set[str],
                                hints: ValidatedRecordsHints,
                                fail_if_cant_handle_hint: bool) -> \
        Tuple[DateOutputStyle, Optional[DateOrderStyle]]:

    # see docs in the types module

    dateformat = hints.dateformat
    timeonlyformat = hints.timeonlyformat
    datetimeformattz = hints.datetimeformattz
    datetimeformat = hints.datetimeformat

    date_order_style: Optional[DateOrderStyle] = None

    if (dateformat == 'YYYY-MM-DD' and
       timeonlyformat in ['HH24:MI:SS', 'HH:MI:SS'] and
       datetimeformattz in ['YYYY-MM-DD HH:MI:SSOF',
                            'YYYY-MM-DD HH24:MI:SSOF'] and
       datetimeformat in ['YYYY-MM-DD HH24:MI:SS',
                          'YYYY-MM-DD HH:MI:SS']):
        date_output_style: DateOutputStyle = 'ISO'
        # date_order_style doesn't really matter, as ISO is not ambiguous
    else:
        # 'SQL', 'Postgres' and 'German' all support only alphabetic
        # timezone indicators, which aren't yet supported in the
        # records spec
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformattz', hints)

    quiet_remove(unhandled_hints, 'dateformat')
    quiet_remove(unhandled_hints, 'timeonlyformat')
    quiet_remove(unhandled_hints, 'datetimeformattz')
    quiet_remove(unhandled_hints, 'datetimeformat')

    return (date_output_style, date_order_style)
