import unittest
from records_mover.records.targets.spectrum import SpectrumRecordsTarget
from records_mover.records.existing_table_handling import ExistingTableHandling
from mock import Mock, patch, MagicMock


class TestSpectrum(unittest.TestCase):
    @patch('records_mover.records.targets.spectrum.ParquetRecordsFormat')
    def setUp(self,
              mock_ParquetRecordsFormat):
        mock_schema_name = 'myschema'
        mock_table_name = 'mytable'
        mock_url_resolver = Mock(name='url_resolver')
        mock_db_driver = Mock(name='db_driver')
        mock_spectrum_base_url = Mock(name='spectrum_base_url')
        self.mock_driver = mock_db_driver.return_value
        self.mock_db = MagicMock(name='db')
        self.mock_driver.db_engine = self.mock_db
        self.mock_output_loc = mock_url_resolver.directory_url.return_value.\
            directory_in_this_directory.return_value.\
            directory_in_this_directory.return_value.\
            directory_in_this_directory.return_value
        self.mock_output_loc.url = 's3://output-loc/'
        self.mock_output_loc.scheme = 's3'
        self.records_format = mock_ParquetRecordsFormat.return_value
        self.target =\
            SpectrumRecordsTarget(schema_name=mock_schema_name,
                                  table_name=mock_table_name,
                                  db_engine=self.mock_db,
                                  db_driver=mock_db_driver,
                                  url_resolver=mock_url_resolver,
                                  spectrum_base_url=mock_spectrum_base_url,
                                  spectrum_rdir_url=None,
                                  existing_table_handling=ExistingTableHandling.DROP_AND_RECREATE)
        mock_url_resolver.directory_url.assert_called_with(mock_spectrum_base_url)

    def test_init(self):
        self.assertEqual(self.target.records_format, self.records_format)
        self.assertEqual(self.target.db, self.mock_db)

    @patch('records_mover.records.targets.spectrum.quote_schema_and_table')
    def test_pre_load_hook_preps_bucket_with_default_prep(self, mock_quote_schema_and_table):
        mock_schema_and_table = mock_quote_schema_and_table.return_value
        mock_cursor = self.target.driver.db_engine.connect.return_value.__enter__.return_value

        self.target.pre_load_hook()
        mock_quote_schema_and_table.assert_called_with(self.target.db,
                                                       self.target.schema_name,
                                                       self.target.table_name)
        mock_cursor.execution_options.assert_called_with(isolation_level='AUTOCOMMIT')
        mock_cursor.execute.assert_called_with(f"DROP TABLE IF EXISTS {mock_schema_and_table}")
        self.mock_output_loc.purge_directory.assert_called_with()

    @patch('records_mover.records.targets.spectrum.RecordsDirectory')
    def test_records_directory(self, mock_RecordsDirectory):
        out = self.target.records_directory()
        mock_RecordsDirectory.assert_called_with(self.mock_output_loc)
        self.assertEqual(out, mock_RecordsDirectory.return_value)

    @patch('records_mover.records.targets.spectrum.CreateTable')
    @patch('records_mover.records.targets.spectrum.Table')
    @patch('records_mover.records.targets.spectrum.MetaData')
    @patch('records_mover.records.targets.spectrum.RecordsDirectory')
    def test_post_load_hook_creates_table(self,
                                          mock_RecordsDirectory,
                                          mock_MetaData,
                                          mock_Table,
                                          mock_CreateTable):
        mock_num_rows_loaded = 123
        mock_directory = mock_RecordsDirectory.return_value
        mock_records_schema = mock_directory.load_schema_json_obj.return_value
        mock_field = Mock(name='field')
        mock_records_schema.fields = [mock_field]
        mock_meta = mock_MetaData.return_value
        mock_columns = [mock_field.to_sqlalchemy_column.return_value]
        mock_table = mock_Table.return_value
        mock_CreateTable.return_value = "SOME GENERATED CREATE TABLES STATEMENT "
        mock_cursor = self.target.driver.db_engine.connect.return_value.__enter__.return_value

        self.target.post_load_hook(num_rows_loaded=mock_num_rows_loaded)
        mock_directory.load_schema_json_obj.assert_called_with()
        mock_directory.get_manifest.assert_called_with()
        mock_field.to_sqlalchemy_column.assert_called_with(self.mock_driver)
        mock_Table.assert_called_with('mytable', mock_meta,
                                      *mock_columns, prefixes=['EXTERNAL'], schema='myschema')
        mock_CreateTable.assert_called_with(mock_table, bind=self.mock_driver.db_engine)
        mock_cursor.execution_options.assert_called_with(isolation_level='AUTOCOMMIT')
        mock_cursor.execute.assert_called_with("SOME GENERATED CREATE TABLES STATEMENT "
                                               "STORED AS PARQUET\n"
                                               "LOCATION 's3://output-loc/_manifest'\n\n"
                                               "TABLE PROPERTIES ('numRows'='123')")
