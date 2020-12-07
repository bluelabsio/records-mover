from ...utils import quiet_remove
from ...records.delimited import cant_handle_hint
from typing import Dict, Set
from ...records.load_plan import RecordsLoadPlan
from ...records.records_format import DelimitedRecordsFormat


VerticaImportOptions = Dict[str, object]


def vertica_import_options(unhandled_hints: Set[str],
                           load_plan: RecordsLoadPlan) -> VerticaImportOptions:
    fail_if_cant_handle_hint = load_plan.processing_instructions.fail_if_cant_handle_hint
    if not isinstance(load_plan.records_format, DelimitedRecordsFormat):
        raise NotImplementedError("Not currently able to import "
                                  f"{load_plan.records_format.format_type}")
    hints = load_plan.records_format.hints

    vertica_options: VerticaImportOptions = {}

    vertica_options['record_terminator'] = hints['record-terminator']
    quiet_remove(unhandled_hints, 'record-terminator')
    if hints['compression'] is None:
        vertica_options['gzip'] = False
    elif hints['compression'] == 'GZIP':
        vertica_options['gzip'] = True
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'compression', hints)

    quiet_remove(unhandled_hints, 'compression')
    vertica_options['escape_as'] = hints['escape']
    quiet_remove(unhandled_hints, 'escape')

    if not hints['timeonlyformat'] in ['HH24:MI:SS',
                                       'HH:MI:SS',
                                       'HH12:MI AM']:
        # Vertica seems to be able to understand these on import automatically
        cant_handle_hint(fail_if_cant_handle_hint, 'timeonlyformat', hints)
    quiet_remove(unhandled_hints, 'timeonlyformat')

    if hints['quoting'] is None:
        vertica_options['enclosed_by'] = None
    elif hints['quoting'] == 'all':
        if hints['doublequote'] is not False:
            cant_handle_hint(fail_if_cant_handle_hint, 'doublequote', hints)
        vertica_options['enclosed_by'] = hints['quotechar']
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'quoting', hints)
    quiet_remove(unhandled_hints, 'doublequote')
    quiet_remove(unhandled_hints, 'quotechar')
    quiet_remove(unhandled_hints, 'quoting')

    if not hints['dateformat'] in ['YYYY-MM-DD']:
        cant_handle_hint(fail_if_cant_handle_hint, 'dateformat', hints)
    quiet_remove(unhandled_hints, 'dateformat')

    vertica_options['delimiter'] = hints['field-delimiter']
    quiet_remove(unhandled_hints, 'field-delimiter')

    if hints['datetimeformattz'] not in ['YYYY-MM-DD HH:MI:SSOF',
                                         'YYYY-MM-DD HH24:MI:SSOF']:
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformattz', hints)
    quiet_remove(unhandled_hints, 'datetimeformattz')

    if not hints['datetimeformat'] in ['YYYY-MM-DD HH:MI:SS', 'YYYY-MM-DD HH24:MI:SS']:
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformat', hints)
    quiet_remove(unhandled_hints, 'datetimeformat')

    if hints['encoding'] != 'UTF8':
        cant_handle_hint(fail_if_cant_handle_hint, 'encoding', hints)
    quiet_remove(unhandled_hints, 'encoding')

    if hints['header-row']:
        vertica_options['skip'] = 1
    else:
        vertica_options['skip'] = 0
    quiet_remove(unhandled_hints, 'header-row')

    vertica_options['load_method'] = 'AUTO'

    vertica_options['no_commit'] = False

    if load_plan.processing_instructions.max_failure_rows is not None:
        vertica_options['trailing_nullcols'] = True
        vertica_options['rejectmax'] = load_plan.processing_instructions.max_failure_rows
        vertica_options['enforcelength'] = None
        vertica_options['error_tolerance'] = None
        vertica_options['abort_on_error'] = None
    elif not load_plan.processing_instructions.fail_if_row_invalid:
        vertica_options['trailing_nullcols'] = True
        vertica_options['rejectmax'] = None
        vertica_options['enforcelength'] = False
        vertica_options['error_tolerance'] = True
        vertica_options['abort_on_error'] = False
    else:
        vertica_options['trailing_nullcols'] = False
        vertica_options['rejectmax'] = 1
        vertica_options['enforcelength'] = True
        vertica_options['error_tolerance'] = False
        vertica_options['abort_on_error'] = True

    vertica_options['null_as'] = None

    return vertica_options
