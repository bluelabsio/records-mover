from records_mover.records.sources.table import TableRecordsSource
from mock import Mock, patch, ANY
import unittest
from records_mover.records.targets.base import MightSupportMoveFromTempLocAfterFillingIt


class TestTableRecordsSourceEdgeCases(unittest.TestCase):
    @patch('records_mover.records.sources.dataframes.DataframesRecordsSource')
    @patch('records_mover.records.sources.table.RecordsSchema')
    @patch('records_mover.records.sources.table.quote_schema_and_table')
    @patch('pandas.read_sql')
    def test_init_no_unloader(self,
                              mock_read_sql,
                              mock_quote_schema_and_table,
                              mock_RecordsSchema,
                              mock_DataframesRecordsSource):
        mock_schema_name = Mock(name='schema_name')
        mock_table_name = Mock(name='table_name')
        mock_driver = Mock(name='driver')
        mock_unloader = None
        mock_driver.unloader.return_value = mock_unloader
        mock_url_resolver = Mock(name='url_resolver')
        table_records_source =\
            TableRecordsSource(schema_name=mock_schema_name,
                               table_name=mock_table_name,
                               driver=mock_driver,
                               url_resolver=mock_url_resolver)
        self.assertIsNone(table_records_source.records_format)
        mock_target_records_format = Mock(name='target_records_format')
        self.assertFalse(table_records_source.can_move_to_format(mock_target_records_format))
