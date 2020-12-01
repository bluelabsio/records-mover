import unittest
from records_mover.records.records_format import AvroRecordsFormat


class TestRecordsFormat(unittest.TestCase):
    def test_AvroRecordsFormat_str(self):
        records_format = AvroRecordsFormat()
        self.assertEqual(str(records_format), 'AvroRecordsFormat')

    def test_AvroRecordsFormat_repr(self):
        records_format = AvroRecordsFormat()
        self.assertEqual(repr(records_format), 'AvroRecordsFormat')
