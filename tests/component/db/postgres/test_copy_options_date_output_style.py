import unittest
from records_mover.records import DelimitedRecordsFormat
from records_mover.db.postgres.copy_options.date_output_style import\
    determine_date_output_style
from ...records.datetime_cases import (
    DATE_CASES, DATETIMEFORMATTZ_CASES,
    create_sample, SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY
)


class TestPostgresCopyOptionsDateOutputStyle(unittest.TestCase):
    def test_determine_output_date_order_style_iso_1(self):
        unhandled_hints = set()
        # TODO: Shouldn't variations of this that include HH or HH24
        # also work?
        records_format = DelimitedRecordsFormat(hints={
            'dateformat': 'YYYY-MM-DD',
            'timeonlyformat': 'HH24:MI:SS',
            'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
            'datetimeformat': 'YYYY-MM-DD HH24:MI:SS'
        })
        fail_if_cant_handle_hint = True
        validated_hints = records_format.validate(fail_if_cant_handle_hint=fail_if_cant_handle_hint)

        out = determine_date_output_style(unhandled_hints,
                                          validated_hints,
                                          fail_if_cant_handle_hint)
        self.assertEqual(out, ('ISO', None))

    def test_determine_output_date_order_style_datetime(self):
        unhandled_hints = set()
        # Records Mover only supports Postgres in ISO format at this
        # point (YYYY-MM-DD) - see comments in types.py and in
        # date_output_style.py for more detail.
        expected_failures = {
            'MM-DD-YYYY',
            'DD-MM-YYYY',
            'MM/DD/YY',
            'DD/MM/YY',
            'DD-MM-YY',
        }
        for dateformat in DATE_CASES:
            records_format = DelimitedRecordsFormat(hints={
                'dateformat': dateformat,
                'timeonlyformat': 'HH24:MI:SS',
                'datetimeformattz': f'{dateformat} HH:MI:SSOF',
                'datetimeformat': f'{dateformat} HH24:MI:SS'
            })
            fail_if_cant_handle_hint = True
            validated_hints =\
                records_format.validate(fail_if_cant_handle_hint=fail_if_cant_handle_hint)
            try:
                out = determine_date_output_style(unhandled_hints,
                                                  validated_hints,
                                                  fail_if_cant_handle_hint)
            except NotImplementedError:
                if dateformat in expected_failures:
                    pass
                else:
                    raise
            self.assertEqual(out, ('ISO', None))

    def test_determine_output_date_order_style_datetimeformattz(self):
        unhandled_hints = set()
        # Records Mover only supports Postgres in ISO format at this
        # point (YYYY-MM-DD) - see comments in types.py and in
        # date_output_style.py for more detail.
        expected_failures = {
            # no timezone, even though otherwise in ISO format
            'YYYY-MM-DD HH:MI:SS',
            # not in ISO format
            'MM/DD/YY HH24:MI',
        }
        natural_dateformat = {
            'YYYY-MM-DD HH:MI:SSOF': 'YYYY-MM-DD',
            'YYYY-MM-DD HH:MI:SS': 'YYYY-MM-DD',
            'YYYY-MM-DD HH24:MI:SSOF': 'YYYY-MM-DD',
            'MM/DD/YY HH24:MI': 'MM/DD/YY',
        }
        natural_timeonlyformat = {
            'YYYY-MM-DD HH:MI:SSOF': 'HH:MI:SS',
            'YYYY-MM-DD HH:MI:SS': 'HH:MI:SS',
            'YYYY-MM-DD HH24:MI:SSOF': 'HH24:MI:SS',
            'MM/DD/YY HH24:MI': 'HH24:MI',
        }
        natural_datetimeformat = {
            'YYYY-MM-DD HH:MI:SSOF': 'YYYY-MM-DD HH:MI:SS',
            'YYYY-MM-DD HH:MI:SS': 'YYYY-MM-DD HH:MI:SS',
            'YYYY-MM-DD HH24:MI:SSOF': 'YYYY-MM-DD HH24:MI:SS',
            'MM/DD/YY HH24:MI': 'HH24:MI',
        }
        for datetimeformattz in DATETIMEFORMATTZ_CASES:
            records_format = DelimitedRecordsFormat(hints={
                'dateformat': natural_dateformat[datetimeformattz],
                'timeonlyformat': natural_timeonlyformat[datetimeformattz],
                'datetimeformattz': datetimeformattz,
                'datetimeformat': natural_datetimeformat[datetimeformattz],
            })
            fail_if_cant_handle_hint = True
            validated_hints =\
                records_format.validate(fail_if_cant_handle_hint=fail_if_cant_handle_hint)
            try:
                out = determine_date_output_style(unhandled_hints,
                                                  validated_hints,
                                                  fail_if_cant_handle_hint)
            except NotImplementedError:
                if datetimeformattz in expected_failures:
                    pass
                else:
                    raise
            self.assertEqual(out, ('ISO', None))
