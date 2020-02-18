from mock import Mock, patch
from .base_test_records import BaseTestRecords


@patch('records_mover.records.sources.directory.RecordsDirectory')
class TestRecords(BaseTestRecords):
    def test_best_records_format_variant(self, mock_RecordsDirectory):
        mock_records_format_type = Mock(name='mock_records_format_type')
        mock_db_engine = Mock(name='db_engine')
        out = self.records.best_records_format_variant(mock_records_format_type,
                                                       mock_db_engine)
        self.mock_db_driver.best_records_format_variant.assert_called_with(mock_records_format_type)
        self.assertEqual(out, self.mock_db_driver.best_records_format_variant.return_value)
