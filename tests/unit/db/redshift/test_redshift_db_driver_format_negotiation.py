from records_mover.db.redshift.redshift_db_driver import RedshiftDBDriver
from records_mover.records.records_format import DelimitedRecordsFormat
import unittest
from mock import patch, MagicMock, Mock


def fake_text(s):
    return (s,)


class TestRedshiftDBDriverFormatNegotiation(unittest.TestCase):
    @patch('records_mover.db.redshift.redshift_db_driver.RedshiftLoader')
    @patch('records_mover.db.redshift.redshift_db_driver.RedshiftUnloader')
    def setUp(self, mock_RedshiftUnloader, mock_RedshiftLoader):
        mock_db_engine = MagicMock(name='db_engine')
        mock_db_engine.engine = mock_db_engine
        self.mock_redshift_loader = mock_RedshiftLoader.return_value
        self.mock_redshift_unloader = mock_RedshiftUnloader.return_value
        mock_s3_temp_base_loc = MagicMock(name='s3_temp_base_loc')
        mock_s3_temp_base_loc.url = 's3://fakebucket/fakedir/fakesubdir/'
        mock_db_engine.dialect.\
            preparer.return_value.\
            quote.return_value.\
            __add__.return_value.\
            __add__.return_value = 'myschema.mytable'
        self.redshift_db_driver = RedshiftDBDriver(db=mock_db_engine,
                                                   s3_temp_base_loc=mock_s3_temp_base_loc)

    def test_can_load_this_format(self):
        mock_source_records_format = Mock(name='source_records_format', spec=DelimitedRecordsFormat)
        out = self.redshift_db_driver.loader().can_load_this_format(mock_source_records_format)
        self.mock_redshift_loader.can_load_this_format.\
            assert_called_with(mock_source_records_format)
        self.assertEqual(out, self.mock_redshift_loader.can_load_this_format.return_value)

    def test_can_unload_this_format(self):
        mock_source_records_format = Mock(name='source_records_format', spec=DelimitedRecordsFormat)
        out = self.redshift_db_driver.unloader().\
            can_unload_this_format(mock_source_records_format)
        self.mock_redshift_unloader.can_unload_this_format.\
            assert_called_with(mock_source_records_format)
        self.assertEqual(out, self.mock_redshift_unloader.can_unload_this_format.return_value)
