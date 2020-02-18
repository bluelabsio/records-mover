import unittest
from mock import Mock, MagicMock
from records_mover.records.records_format import DelimitedRecordsFormat
from records_mover.db.unloader import Unloader


class TestUnloader(unittest.TestCase):
    def setUp(self):
        self.mock_db_engine = MagicMock()
        self.unloader = Unloader(db=self.mock_db_engine)

    def test_unload(self):
        mock_schema = Mock(name='schema')
        mock_table = Mock(name='table')
        mock_unload_plan = Mock(name='unload_plan')
        mock_unload_plan.records_format = Mock(spec=DelimitedRecordsFormat)
        mock_unload_plan.records_format.hints = {
            'compression': 'GZIP'
        }
        mock_directory = Mock(name='directory')
        mock_column = Mock(name='column')
        self.mock_db_engine.dialect.get_columns.return_value = [mock_column]
        mock_df = Mock(name='df')
        mock_df.shape = MagicMock(name='shape')
        with self.assertRaises(NotImplementedError):
            self.unloader.unload(schema=mock_schema,
                                 table=mock_table,
                                 unload_plan=mock_unload_plan,
                                 directory=mock_directory)

    def test_can_unload_this_format(self):
        mock_db = Mock(name='db')
        mock_target_records_format = Mock(name='target_records_format', spec=DelimitedRecordsFormat)
        mock_target_records_format.hints = {}

        unloader = Unloader(db=mock_db)
        out = unloader.can_unload_this_format(mock_target_records_format)
        self.assertEqual(out, False)
