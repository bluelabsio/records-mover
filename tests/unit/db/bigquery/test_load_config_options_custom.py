import unittest

from records_mover.db.bigquery.load_job_config_options import load_job_config
from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.records_format import DelimitedRecordsFormat


class TestLoadJobConfigCustom(unittest.TestCase):

    def test_load_job_config_unsupported_encoding(self):
        records_format = DelimitedRecordsFormat(variant='bigquery',
                                                hints={
                                                    'encoding': 'somethingunusual'
                                                })
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=records_format)
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaises(NotImplementedError):
            load_job_config(unhandled_hints, load_plan)

    def test_load_job_config_custom_failure_rows(self):
        records_format = DelimitedRecordsFormat(variant='bigquery')
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True,
                                                         max_failure_rows=123)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=records_format)
        unhandled_hints = set(records_format.hints.keys())
        out = load_job_config(unhandled_hints, load_plan)
        expectations = {
            'allowJaggedRows': True,
            'allowQuotedNewlines': True,
            'autodetect': False,
            'createDisposition': 'CREATE_NEVER',
            'destinationTableProperties': {},
            'encoding': 'UTF-8',
            'fieldDelimiter': ',',
            'ignoreUnknownValues': True,
            'maxBadRecords': 123,
            'quote': '"',
            'schemaUpdateOptions': None,
            'skipLeadingRows': '1',
            'sourceFormat': 'CSV',
            'writeDisposition': 'WRITE_APPEND'
        }
        self.assertEqual(out.to_api_repr()['load'], expectations)

    def test_load_job_config_permissive(self):
        records_format = DelimitedRecordsFormat(variant='bigquery')
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=False)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=records_format)
        unhandled_hints = set(records_format.hints.keys())
        out = load_job_config(unhandled_hints, load_plan)
        expectations = {
            'allowJaggedRows': True,
            'allowQuotedNewlines': True,
            'autodetect': False,
            'createDisposition': 'CREATE_NEVER',
            'destinationTableProperties': {},
            'encoding': 'UTF-8',
            'fieldDelimiter': ',',
            'ignoreUnknownValues': False,
            'maxBadRecords': 999999,
            'quote': '"',
            'schemaUpdateOptions': None,
            'skipLeadingRows': '1',
            'sourceFormat': 'CSV',
            'writeDisposition': 'WRITE_APPEND'
        }
        self.assertEqual(out.to_api_repr()['load'], expectations)

    def test_load_job_config_unsupported_no_doublequote(self):
        records_format = DelimitedRecordsFormat(variant='bigquery',
                                                hints={
                                                    'doublequote': False
                                                })
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=records_format)
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaises(NotImplementedError):
            load_job_config(unhandled_hints, load_plan)

    def test_load_job_config_unknown_quoting(self):
        records_format = DelimitedRecordsFormat(variant='bigquery',
                                                hints={
                                                    'quoting': 'blah'
                                                })
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=records_format)
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaises(NotImplementedError):
            load_job_config(unhandled_hints, load_plan)

    def test_load_job_config_unsupported_datetimeformat(self):
        records_format = DelimitedRecordsFormat(variant='bigquery',
                                                hints={
                                                    'datetimeformat': 'YYYY-MM-DD HH12:MI:SS AM'
                                                })
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=records_format)
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaisesRegex(NotImplementedError,
                                    r"Implement hint datetimeformat='YYYY-MM-DD HH12:MI:SS AM' "
                                    "or try again with fail_if_cant_handle_hint=False"):
            load_job_config(unhandled_hints, load_plan)

    def test_load_job_config_unsupported_datetimeformattz(self):
        records_format = DelimitedRecordsFormat(variant='bigquery',
                                                hints={
                                                    'datetimeformattz': 'MM/DD/YY HH:MI:SSOF'
                                                })
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=records_format)
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaisesRegex(NotImplementedError,
                                    r"Implement hint datetimeformattz='MM/DD/YY HH:MI:SSOF' "
                                    "or try again with fail_if_cant_handle_hint=False"):
            load_job_config(unhandled_hints, load_plan)

    def test_load_job_config_unsupported_timeonlyformat(self):
        records_format = DelimitedRecordsFormat(variant='bigquery',
                                                hints={
                                                    'timeonlyformat': 'HH12:MI:SS AM'
                                                })
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=records_format)
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaisesRegex(NotImplementedError,
                                    r"Implement hint timeonlyformat='HH12:MI:SS AM' "
                                    "or try again with fail_if_cant_handle_hint=False"):
            load_job_config(unhandled_hints, load_plan)

    def test_load_job_config_no_bzip_support(self):
        records_format = DelimitedRecordsFormat(variant='bigquery',
                                                hints={
                                                    'compression': 'BZIP'
                                                })
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=records_format)
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaisesRegex(NotImplementedError,
                                    r"Implement hint compression='BZIP' "
                                    "or try again with fail_if_cant_handle_hint=False"):
            load_job_config(unhandled_hints, load_plan)
