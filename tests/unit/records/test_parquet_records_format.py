from records_mover.records.records_format import ParquetRecordsFormat
import unittest


class TestParquetRecordsFormat(unittest.TestCase):
    def test_init(self):
        out = ParquetRecordsFormat()
        self.assertIsNotNone(out)

    def test_str(self):
        out = ParquetRecordsFormat()
        self.assertEqual(str(out), "ParquetRecordsFormat")

    def test_repr(self):
        out = ParquetRecordsFormat()
        self.assertEqual(repr(out), "ParquetRecordsFormat")

    def test_generate_filename(self):
        out = ParquetRecordsFormat()
        self.assertEqual(out.generate_filename("foo"), "foo.parquet")
