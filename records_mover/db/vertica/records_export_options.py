from ...utils import quiet_remove
from ...records.delimited import cant_handle_hint
from ...records.unload_plan import RecordsUnloadPlan
from ...records.records_format import DelimitedRecordsFormat
from typing import Set, Dict, Any


def vertica_export_options(unhandled_hints: Set[str],
                           unload_plan: RecordsUnloadPlan) -> Dict[str, Any]:
    if not isinstance(unload_plan.records_format, DelimitedRecordsFormat):
        raise NotImplementedError("Not currently able to export "
                                  f"{unload_plan.records_format.format_type}")

    hints = unload_plan.records_format.hints
    fail_if_cant_handle_hint = unload_plan.processing_instructions.fail_if_cant_handle_hint

    vertica_options = {}

    vertica_options['to_charset'] = hints['encoding']
    quiet_remove(unhandled_hints, 'encoding')

    if hints['compression'] is not None:
        cant_handle_hint(fail_if_cant_handle_hint, 'compression', hints)
    quiet_remove(unhandled_hints, 'compression')

    if hints['quoting'] is not None:
        cant_handle_hint(fail_if_cant_handle_hint, 'quoting', hints)
    quiet_remove(unhandled_hints, 'quoting')
    quiet_remove(unhandled_hints, 'doublequote')
    quiet_remove(unhandled_hints, 'quotechar')

    # Vertica does no escaping with S3EXPORT:
    #   https://my.vertica.com/docs/8.1.x/HTML/index.htm#Authoring/
    #     SQLReferenceManual/Functions/VerticaFunctions/s3export.htm
    if hints['escape'] is not None:
        cant_handle_hint(fail_if_cant_handle_hint, 'escape', hints)
    quiet_remove(unhandled_hints, 'escape')

    if hints['header-row'] is not False:
        cant_handle_hint(fail_if_cant_handle_hint, 'header-row', hints)
    quiet_remove(unhandled_hints, 'header-row')

    if hints['dateformat'] != 'YYYY-MM-DD':
        cant_handle_hint(fail_if_cant_handle_hint, 'dateformat', hints)
    quiet_remove(unhandled_hints, 'dateformat')

    if hints['datetimeformattz'] not in ['YYYY-MM-DD HH:MI:SSOF',
                                         'YYYY-MM-DD HH24:MI:SSOF']:
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformattz', hints)
    quiet_remove(unhandled_hints, 'datetimeformattz')

    if hints['datetimeformat'] not in ['YYYY-MM-DD HH:MI:SS',
                                       'YYYY-MM-DD HH24:MI:SS']:
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformat', hints)
    quiet_remove(unhandled_hints, 'datetimeformat')

    if hints['timeonlyformat'] not in ['HH24:MI:SS',
                                       'HH:MI:SS']:
        cant_handle_hint(fail_if_cant_handle_hint, 'timeonlyformat', hints)
    quiet_remove(unhandled_hints, 'timeonlyformat')

    vertica_options['delimiter'] = hints['field-delimiter']
    quiet_remove(unhandled_hints, 'field-delimiter')

    vertica_options['record_terminator'] = hints['record-terminator']
    quiet_remove(unhandled_hints, 'record-terminator')

    return vertica_options
