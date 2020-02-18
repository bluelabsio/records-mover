import unittest
from mock import Mock
from records_mover.records.schema.field.statistics import (RecordsSchemaFieldStatistics)


class TestStatistics(unittest.TestCase):
    maxDiff = None

    def test_init(self):
        mock_rows_sampled = Mock(name='rows_sampled')
        mock_total_rows = Mock(name='total_rows')

        RecordsSchemaFieldStatistics(rows_sampled=mock_rows_sampled,
                                     total_rows=mock_total_rows)

    def test_to_data(self):
        mock_rows_sampled = Mock(name='rows_sampled')
        mock_total_rows = Mock(name='total_rows')

        stats = RecordsSchemaFieldStatistics(rows_sampled=mock_rows_sampled,
                                             total_rows=mock_total_rows)
        self.assertEqual(stats.to_data(), {
            'rows_sampled': mock_rows_sampled,
            'total_rows': mock_total_rows
        })

    def test_from_data_integer(self):
        d = {
            'rows_sampled': 123,
            'total_rows': 123
        }
        stats = RecordsSchemaFieldStatistics.from_data(d, 'integer')
        self.assertEqual(stats.rows_sampled, 123)
        self.assertEqual(stats.total_rows, 123)
