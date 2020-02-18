import unittest

from records_mover.records.hints import complain_on_unhandled_hints
from records_mover.db.bigquery.load_job_config_options import load_job_config
from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.records_format import DelimitedRecordsFormat


class TestLoadJobConfigVariants(unittest.TestCase):
    def test_load_job_config_bigquery_variant(self):
        records_format = DelimitedRecordsFormat(variant='bigquery')
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=records_format)
        unhandled_hints = set(records_format.hints.keys())
        out = load_job_config(unhandled_hints, load_plan)
        complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                    unhandled_hints, records_format.hints)
        expectations = {
            'allowJaggedRows': False,
            'allowQuotedNewlines': True,
            'autodetect': False,
            'createDisposition': 'CREATE_NEVER',
            'destinationTableProperties': {},
            'encoding': 'UTF-8',
            'fieldDelimiter': ',',
            'ignoreUnknownValues': True,
            'maxBadRecords': 0,
            'quote': '"',
            'schemaUpdateOptions': None,
            'skipLeadingRows': '1',
            'sourceFormat': 'CSV',
            'writeDisposition': 'WRITE_APPEND'
        }
        self.assertEqual(out.to_api_repr()['load'], expectations)

    def test_load_job_config_dumb_variant(self):
        records_format = DelimitedRecordsFormat(variant='dumb')
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=records_format)
        unhandled_hints = set(records_format.hints.keys())
        out = load_job_config(unhandled_hints, load_plan)
        complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                    unhandled_hints, records_format.hints)
        expectations = {
            'allowJaggedRows': False,
            'autodetect': False,
            'createDisposition': 'CREATE_NEVER',
            'destinationTableProperties': {},
            'encoding': 'UTF-8',
            'fieldDelimiter': ',',
            'ignoreUnknownValues': True,
            'maxBadRecords': 0,
            'quote': '',
            'schemaUpdateOptions': None,
            'skipLeadingRows': '0',
            'sourceFormat': 'CSV',
            'writeDisposition': 'WRITE_APPEND'
        }
        self.assertEqual(out.to_api_repr()['load'], expectations)

    def test_load_job_config_bluelabs(self):
        records_format = DelimitedRecordsFormat(variant='bluelabs')
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=records_format)
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaisesRegex(NotImplementedError,
                                    r"Implement hint escape='\\\\' or try again "
                                    "with fail_if_cant_handle_hint=False"):
            load_job_config(unhandled_hints, load_plan)

    def test_load_job_config_csv(self):
        records_format = DelimitedRecordsFormat(variant='csv')
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=records_format)
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaisesRegex(NotImplementedError,
                                    r"Implement hint dateformat='MM/DD/YY' or try again "
                                    "with fail_if_cant_handle_hint=False"):
            load_job_config(unhandled_hints, load_plan)

    def test_load_job_config_vertica(self):
        records_format = DelimitedRecordsFormat(variant='vertica')
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=records_format)
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaisesRegex(NotImplementedError,
                                    r"Implement hint record-terminator='\\x02' "
                                    "or try again with fail_if_cant_handle_hint=False"):
            load_job_config(unhandled_hints, load_plan)
