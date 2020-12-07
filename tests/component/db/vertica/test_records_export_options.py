import unittest
from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.db.vertica.records_export_options import vertica_export_options
from records_mover.records import DelimitedRecordsFormat, ProcessingInstructions
from ...records.datetime_cases import (
    DATE_CASES, create_sample, SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY
)


class TestVerticaExportOptions(unittest.TestCase):
    maxDiff = None

    def test_vertica_export_options_datetime(self):
        should_raise = {
            'YYYY-MM-DD': False,
            'MM-DD-YYYY': True,
            'DD-MM-YYYY': True,
            'MM/DD/YY': True,
            'DD/MM/YY': True,
            'DD-MM-YY': True,
        }
        for dateformat in DATE_CASES:
            records_format = DelimitedRecordsFormat(variant='vertica',
                                                    hints={
                                                        'dateformat': dateformat
                                                    })
            unhandled_hints = set(records_format.hints)
            processing_instructions = ProcessingInstructions(max_failure_rows=123)
            load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                        records_format=records_format)
            # Records Mover passes no particular option for dateformat on
            # export in Vertica; it always uses YYYY-MM-DD as a result.
            try:
                vertica_export_options(unhandled_hints, load_plan)
            except NotImplementedError:
                if should_raise[dateformat]:
                    pass
                else:
                    self.fail()
