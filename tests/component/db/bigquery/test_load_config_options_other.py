import unittest

from records_mover.records.delimited import complain_on_unhandled_hints
from records_mover.db.bigquery.load_job_config_options import load_job_config
from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.records_format import ParquetRecordsFormat


class TestLoadJobConfigOther(unittest.TestCase):
    def test_load_job_config_parquet(self):
        records_format = ParquetRecordsFormat()
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                    records_format=records_format)
        unhandled_hints = set()
        out = load_job_config(unhandled_hints, load_plan)
        expectations = {
            'allowJaggedRows': False,
            'autodetect': False,
            'createDisposition': 'CREATE_NEVER',
            'destinationTableProperties': {},
            'ignoreUnknownValues': True,
            'maxBadRecords': 0,
            'schemaUpdateOptions': None,
            'sourceFormat': 'PARQUET',
            'writeDisposition': 'WRITE_APPEND'
        }
        self.assertEqual(expectations, out.to_api_repr()['load'])
