import unittest

from records_mover.db.bigquery.load_job_config_options import load_job_config
from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.records_format import DelimitedRecordsFormat
from ...records.datetime_cases import (
    DATE_CASES, DATETIMEFORMATTZ_CASES, DATETIMEFORMAT_CASES, TIMEONLY_CASES,
    create_sample, SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY, SAMPLE_HOUR, SAMPLE_MINUTE, SAMPLE_SECOND
)


class TestLoadJobConfigDatetime(unittest.TestCase):
    def test_dateformat(self):
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        # Double check this before adding anything else in here to see
        # if it has changed, but YYYY-MM-DD is the only format
        # accepted by BigQuery as of this writing
        should_raise = {
            'YYYY-MM-DD': False,
            'MM-DD-YYYY': True,
            'DD-MM-YYYY': True,
            'MM/DD/YY': True,
            'DD/MM/YY': True,
            'DD-MM-YY': True,
        }
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
                if should_raise[dateformat]:
                    pass
                else:
                    raise

    def test_datetimeformattz(self):
        # Double check this before adding anything else in here to see
        # if it has changed, but YYYY-MM-DD HH:MI:SSOF, YYYY-MM-DD
        # HH24:MI:SSOF and YYYY-MM-DD HH:MI:SS are the only
        # formats accepted by BigQuery as of this writing
        should_raise = {
            'YYYY-MM-DD HH:MI:SSOF': False,
            'YYYY-MM-DD HH24:MI:SSOF': False,
            'YYYY-MM-DD HH:MI:SS': False,
            'MM/DD/YY HH24:MI': True
        }
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        for datetimeformattz in DATETIMEFORMATTZ_CASES:
            records_format =\
                DelimitedRecordsFormat(variant='bigquery',
                                       hints={
                                           'datetimeformattz': datetimeformattz
                                       })
            load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                        records_format=records_format)
            unhandled_hints = set(records_format.hints.keys())
            try:
                load_job_config(unhandled_hints, load_plan)
            except NotImplementedError:
                if should_raise[datetimeformattz]:
                    pass
                else:
                    raise

    def test_datetimeformat(self):
        # Double check this before adding anything else in here to see
        # if it has changed, but YYYY-MM-DD HH:MI:SS, YYYY-MM-DD
        # HH24:MI:SS and YYYY-MM-DD HH:MI:SS are the only formats
        # accepted by BigQuery as of this writing
        should_raise = {
            'YYYY-MM-DD HH12:MI AM': True,
            'MM/DD/YY HH24:MI': True,
        }
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        for datetimeformat in DATETIMEFORMAT_CASES:
            records_format =\
                DelimitedRecordsFormat(variant='bigquery',
                                       hints={
                                           'datetimeformat': datetimeformat
                                       })
            load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                        records_format=records_format)
            unhandled_hints = set(records_format.hints.keys())
            try:
                load_job_config(unhandled_hints, load_plan)
            except NotImplementedError:
                if should_raise[datetimeformat]:
                    pass
                else:
                    raise

    def test_timeonlyformat(self):
        # Double check this before adding anything else in here to see
        # if it has changed, but HH:MI:SS is the only format accepted
        # by BigQuery as of this writing
        should_raise = {
            'HH:MI:SS': False,
            'HH24:MI:SS': False,
            'HH12:MI AM': True,
        }
        processing_instructions = ProcessingInstructions(fail_if_dont_understand=True,
                                                         fail_if_cant_handle_hint=True,
                                                         fail_if_row_invalid=True)
        for timeonlyformat in TIMEONLY_CASES:
            records_format =\
                DelimitedRecordsFormat(variant='bigquery',
                                       hints={
                                           'timeonlyformat': timeonlyformat,
                                       })
            load_plan = RecordsLoadPlan(processing_instructions=processing_instructions,
                                        records_format=records_format)
            unhandled_hints = set(records_format.hints.keys())
            try:
                load_job_config(unhandled_hints, load_plan)
            except NotImplementedError:
                if should_raise[timeonlyformat]:
                    pass
                else:
                    raise
