import unittest
from records_mover.records import DelimitedRecordsFormat
from records_mover.db.postgres.copy_options.date_output_style import\
    determine_date_output_style
from ...records.datetime_cases import (
    DATE_CASES, DATETIMETZ_CASES, DATETIME_CASES, TIMEONLY_CASES,
)


class TestPostgresCopyOptionsDateOutputStyle(unittest.TestCase):
    def test_determine_output_date_order_style_iso_1(self):
        unhandled_hints = set()
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

    def test_determine_output_date_order_style_iso_2(self):
        unhandled_hints = set()
        records_format = DelimitedRecordsFormat(hints={
            'dateformat': 'YYYY-MM-DD',
            'timeonlyformat': 'HH24:MI:SS',
            'datetimeformattz': 'YYYY-MM-DD HH24:MI:SSOF',
            'datetimeformat': 'YYYY-MM-DD HH24:MI:SS'
        })
        fail_if_cant_handle_hint = True
        validated_hints = records_format.validate(fail_if_cant_handle_hint=fail_if_cant_handle_hint)

        out = determine_date_output_style(unhandled_hints,
                                          validated_hints,
                                          fail_if_cant_handle_hint)
        self.assertEqual(out, ('ISO', None))

    def test_determine_output_date_order_style_iso_3(self):
        unhandled_hints = set()
        records_format = DelimitedRecordsFormat(hints={
            'dateformat': 'YYYY-MM-DD',
            'timeonlyformat': 'HH:MI:SS',
            'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
            'datetimeformat': 'YYYY-MM-DD HH:MI:SS'
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
            'MM/DD/YY HH24:MI': 'MM/DD/YY HH24:MI',
        }
        for datetimeformattz in DATETIMETZ_CASES:
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

    def test_determine_output_date_order_style_datetimeformat(self):
        unhandled_hints = set()
        # Records Mover only supports Postgres in ISO format at this
        # point (YYYY-MM-DD) - see comments in types.py and in
        # date_output_style.py for more detail.
        expected_failures = {
            # no timezone, even though otherwise in ISO format
            'YYYY-MM-DD HH:MI:SS',
            # not in ISO format
            'MM/DD/YY HH24:MI',
            # not in ISO format
            'YYYY-MM-DD HH12:MI AM',
        }
        natural_dateformat = {
            'YYYY-MM-DD HH:MI:SS': 'YYYY-MM-DD',
            'MM/DD/YY HH24:MI': 'MM/DD/YY',
            'YYYY-MM-DD HH24:MI:SS': 'YYYY-MM-DD',
            'YYYY-MM-DD HH12:MI AM': 'YYYY-MM-DD',
        }
        natural_timeonlyformat = {
            'YYYY-MM-DD HH:MI:SS': 'HH:MI:SS',
            'MM/DD/YY HH24:MI': 'HH24:MI',
            'YYYY-MM-DD HH24:MI:SS': 'HH24:MI:SS',
            'YYYY-MM-DD HH12:MI AM': 'HH12:MI AM',
        }
        natural_datetimeformattz = {
            'YYYY-MM-DD HH:MI:SS': 'YYYY-MM-DD HH:MI:SSOF',
            'MM/DD/YY HH24:MI': 'MM/DD/YY HH24:MIOF',
            'YYYY-MM-DD HH24:MI:SS': 'YYYY-MM-DD HH24:MI:SSOF',
            'YYYY-MM-DD HH12:MI AM': 'YYYY-MM-DD HH12:MI AM'
        }
        for datetimeformat in DATETIME_CASES:
            records_format = DelimitedRecordsFormat(hints={
                'dateformat': natural_dateformat[datetimeformat],
                'timeonlyformat': natural_timeonlyformat[datetimeformat],
                'datetimeformattz': natural_datetimeformattz[datetimeformat],
                'datetimeformat': datetimeformat,
            })
            fail_if_cant_handle_hint = True
            validated_hints =\
                records_format.validate(fail_if_cant_handle_hint=fail_if_cant_handle_hint)
            try:
                out = determine_date_output_style(unhandled_hints,
                                                  validated_hints,
                                                  fail_if_cant_handle_hint)
            except NotImplementedError:
                if datetimeformat in expected_failures:
                    pass
                else:
                    raise
            self.assertEqual(out, ('ISO', None))

    def test_determine_output_date_order_style_timeonlyformat(self):
        unhandled_hints = set()
        # Records Mover only supports Postgres in ISO format at this
        # point (HH24:MI:SS aka HH:MI:SS) - see comments in types.py and in
        # date_output_style.py for more detail.
        expected_failures = {
            # only HH:MM:SS/HH24MM:SS are supported via the 'ISO'
            # format output
            'HH12:MI AM'
        }
        for timeonlyformat in TIMEONLY_CASES:
            if 'AM' in timeonlyformat:
                datetimeformattz = f"YYYY-MM-DD {timeonlyformat}"
            else:
                datetimeformattz = f"YYYY-MM-DD {timeonlyformat}OF"
            records_format = DelimitedRecordsFormat(hints={
                'dateformat': "YYYY-MM-DD",
                'timeonlyformat': timeonlyformat,
                'datetimeformattz': datetimeformattz,
                'datetimeformat': f"YYYY-MM-DD {timeonlyformat}",
            })
            fail_if_cant_handle_hint = True
            validated_hints =\
                records_format.validate(fail_if_cant_handle_hint=fail_if_cant_handle_hint)
            out = None
            try:
                out = determine_date_output_style(unhandled_hints,
                                                  validated_hints,
                                                  fail_if_cant_handle_hint)
            except NotImplementedError:
                if timeonlyformat in expected_failures:
                    pass
                else:
                    raise
            if out is not None:
                self.assertEqual(out, ('ISO', None))
