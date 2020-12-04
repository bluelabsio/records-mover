import unittest

from records_mover.records.delimited import complain_on_unhandled_hints
from records_mover.db.bigquery.load_job_config_options import load_job_config
from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.records_format import DelimitedRecordsFormat
from ...records.datetime_cases import (
    DATE_CASES, create_sample, SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY
)


class TestLoadJobConfigDatetime(unittest.TestCase):
    def test_dateformat(self):
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        for dateformat in DATE_CASES:
            records_format =\
                DelimitedRecordsFormat(variant='bigquery',
                                       hints={
                                           'dateformat': dateformat
                                       })
            load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                        records_format=records_format)
            unhandled_hints = set(records_format.hints.keys())
            try:
                load_job_config(unhandled_hints, load_plan)
            except NotImplementedError:
                if dateformat == 'YYYY-MM-DD':
                    raise
                else:
                    # expected - YYYY-MM-DD is the only format
                    # accepted by BigQuery
                    pass
