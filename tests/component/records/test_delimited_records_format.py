from records_mover.records.records_format import DelimitedRecordsFormat
import unittest
import json


class TestDelimitedRecordsFormat(unittest.TestCase):
    def test_dumb(self):
        records_format = DelimitedRecordsFormat(variant='dumb')
        # Should match up with
        # https://github.com/bluelabsio/records-mover/blob/master/docs/RECORDS_SPEC.md#dumb-variant
        expected_hints = {
            'compression': 'GZIP',
            'dateformat': 'YYYY-MM-DD',
            'datetimeformat': 'YYYY-MM-DD HH:MI:SS',
            'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
            'doublequote': False,
            'encoding': 'UTF8',
            'escape': None,
            'field-delimiter': ',',
            'quotechar': '"',
            'quoting': None,
            'record-terminator': '\n',
            'timeonlyformat': 'HH24:MI:SS',
            'header-row': False,
        }
        self.assertEqual(expected_hints, records_format.hints)

    def test_csv(self):
        records_format = DelimitedRecordsFormat(variant='csv')
        # Should match up with
        # https://github.com/bluelabsio/records-mover/blob/master/docs/RECORDS_SPEC.md#csv-variant
        expected_hints = {
            'compression': 'GZIP',
            'dateformat': 'MM/DD/YY',
            'datetimeformat': 'MM/DD/YY HH24:MI',
            'datetimeformattz': 'MM/DD/YY HH24:MI',
            'doublequote': True,
            'encoding': 'UTF8',
            'escape': None,
            'field-delimiter': ',',
            'quotechar': '"',
            'quoting': 'minimal',
            'record-terminator': '\n',
            'timeonlyformat': 'HH24:MI:SS',
            'header-row': True,
        }
        self.assertEqual(expected_hints, records_format.hints)

    def test_with_altered_hints(self):
        records_format = DelimitedRecordsFormat(variant='csv').alter_hints({'quotechar': 'A'})
        # Should match up with
        # https://github.com/bluelabsio/records-mover/blob/master/docs/RECORDS_SPEC.md#csv-variant
        expected_hints = {
            'compression': 'GZIP',
            'dateformat': 'MM/DD/YY',
            'datetimeformat': 'MM/DD/YY HH24:MI',
            'datetimeformattz': 'MM/DD/YY HH24:MI',
            'doublequote': True,
            'encoding': 'UTF8',
            'escape': None,
            'field-delimiter': ',',
            'quotechar': 'A',
            'quoting': 'minimal',
            'record-terminator': '\n',
            'timeonlyformat': 'HH24:MI:SS',
            'header-row': True,
        }
        self.assertEqual(expected_hints, records_format.hints)
        self.assertEqual({'quotechar': 'A'}, records_format.custom_hints)

    def test_eq(self):
        records_format_1 = DelimitedRecordsFormat()
        records_format_2 = DelimitedRecordsFormat()
        self.assertTrue(records_format_1 == records_format_2)

    def test_eq_error(self):
        records_format_1 = DelimitedRecordsFormat()
        records_format_2 = "wrong type"
        self.assertTrue(records_format_1 != records_format_2)

    def test_unsupported_variant(self):
        with self.assertRaises(NotImplementedError):
            DelimitedRecordsFormat(variant='fake_thing_i_just_made_up')

    def test_json(self):
        records_format = DelimitedRecordsFormat()
        self.assertEqual({
            'hints': {
                'compression': 'GZIP',
                'dateformat': 'YYYY-MM-DD',
                'datetimeformat': 'YYYY-MM-DD HH24:MI:SS',
                'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
                'doublequote': False,
                'encoding': 'UTF8',
                'escape': '\\',
                'field-delimiter': ',',
                'header-row': False,
                'quotechar': '"',
                'quoting': None,
                'record-terminator': '\n',
                'timeonlyformat': 'HH24:MI:SS'},
            'type': 'delimited',
            'variant': 'bluelabs'
        }, json.loads(records_format.json()))

    def test_repr(self):
        records_format = DelimitedRecordsFormat()
        self.assertEqual('DelimitedRecordsFormat(bluelabs)', repr(records_format))

    def test_generate_filename_gzip(self):
        records_format = DelimitedRecordsFormat(hints={'compression': 'GZIP'})
        self.assertEqual('foo.csv.gz', records_format.generate_filename('foo'))

    def test_generate_filename_bzip(self):
        records_format = DelimitedRecordsFormat(hints={'compression': 'BZIP'})
        self.assertEqual('foo.csv.bz2', records_format.generate_filename('foo'))

    def test_alter_variant(self):
        records_format = DelimitedRecordsFormat(variant='csv', hints={'compression': 'BZIP'})
        new_records_format = records_format.alter_variant('bigquery')
        self.assertEqual(records_format.variant, 'csv')
        self.assertEqual(new_records_format.variant, 'bigquery')
