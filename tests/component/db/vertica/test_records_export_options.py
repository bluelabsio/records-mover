import unittest
from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.db.vertica.records_export_options import vertica_export_options
from records_mover.records import DelimitedRecordsFormat, ProcessingInstructions
from ...records.datetime_cases import (
    DATE_CASES, DATETIMEFORMATTZ_CASES, DATETIMEFORMAT_CASES, TIMEONLY_CASES,
    create_sample, SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY
)


class TestVerticaExportOptions(unittest.TestCase):
    maxDiff = None

    def test_vertica_export_options_dateformat(self):
        # Vertica doesn't currently allow any configurability on
        # output dateformat.  Check again before adding any test
        # cases here!
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

    def test_vertica_export_options_datetimeformattz(self):
        # Vertica doesn't currently allow any configurability on
        # output datetimeformattz.  Check again before adding any test
        # cases here!
        should_raise = {
            'YYYY-MM-DD HH:MI:SS': True,
            'YYYY-MM-DD HH24:MI:SSOF': False,
            'MM/DD/YY HH24:MI': True,
        }
        for datetimeformattz in DATETIMEFORMATTZ_CASES:
            records_format = DelimitedRecordsFormat(variant='vertica',
                                                    hints={
                                                        'datetimeformattz': datetimeformattz
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
                if should_raise[datetimeformattz]:
                    pass
                else:
                    self.fail()

    def test_vertica_export_options_datetimeformat(self):
        # Vertica doesn't currently allow any configurability on
        # output datetimeformat.  Check again before adding any test
        # cases here!
        should_raise = {
            'YYYY-MM-DD HH:MI:SS': True,
            'YYYY-MM-DD HH24:MI:SS': False,
            'MM/DD/YY HH24:MI': True,
            'YYYY-MM-DD HH12:MI AM': True,
        }
        for datetimeformat in DATETIMEFORMAT_CASES:
            records_format = DelimitedRecordsFormat(variant='vertica',
                                                    hints={
                                                        'datetimeformat': datetimeformat,
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
                if should_raise[datetimeformat]:
                    pass
                else:
                    raise

    def test_vertica_export_options_datetimeformattz(self):
        # Vertica doesn't currently allow any configurability on
        # output datetimeformattz.  Check again before adding any test
        # cases here!
        should_raise = {
            'YYYY-MM-DD HH:MI:SS': True,
            'YYYY-MM-DD HH24:MI:SSOF': False,
            'MM/DD/YY HH24:MI': True,
        }
        for datetimeformattz in DATETIMEFORMATTZ_CASES:
            records_format = DelimitedRecordsFormat(variant='vertica',
                                                    hints={
                                                        'datetimeformattz': datetimeformattz
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
                if should_raise[datetimeformattz]:
                    pass
                else:
                    self.fail()

    def test_vertica_export_options_timeonlyformat(self):
        # Vertica doesn't currently allow any configurability on
        # output timeonlyformat.  Check again before adding any test
        # cases here!
        should_raise = {
            'HH:MI:SS': False,
            'HH24:MI:SS': False,
            'HH24:MI': True,
            'HH12:MI AM': True,
        }
        for timeonlyformat in TIMEONLY_CASES:
            records_format = DelimitedRecordsFormat(variant='vertica',
                                                    hints={
                                                        'timeonlyformat': timeonlyformat,
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
                if should_raise[timeonlyformat]:
                    pass
                else:
                    raise
