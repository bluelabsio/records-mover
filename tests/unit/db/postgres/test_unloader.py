import unittest
from records_mover.db.postgres.unloader import PostgresUnloader
from records_mover.records import DelimitedRecordsFormat
from mock import MagicMock, Mock, patch, ANY


class TestPostgresUnloader(unittest.TestCase):
    def setUp(self):
        self.mock_url_resolver = Mock(name='url_resolver')
        self.mock_db = MagicMock(name='db')
        self.unloader = PostgresUnloader(self.mock_db)

    @patch('records_mover.db.postgres.unloader.quote_value')
    @patch('records_mover.db.postgres.unloader.copy_to')
    @patch('records_mover.db.postgres.unloader.complain_on_unhandled_hints')
    @patch('records_mover.db.postgres.unloader.Table')
    @patch('records_mover.db.postgres.unloader.postgres_copy_to_options')
    def test_unload(self,
                    mock_postgres_copy_to_options,
                    mock_Table,
                    mock_complain_on_unhandled_hints,
                    mock_copy_to,
                    mock_quote_value):
        mock_schema = Mock(name='schema')
        mock_table = Mock(name='table')
        mock_unload_plan = Mock(name='unload_plan')
        mock_directory = MagicMock(name='directory')

        mock_records_format = Mock(name='records_format',
                                   spec=DelimitedRecordsFormat)
        mock_records_format.hints = {}
        mock_unload_plan.records_format = mock_records_format
        mock_date_output_style = "DATE_OUTPUT_STYLE"
        mock_date_order_style = "DATE_ORDER_STYLE"
        mock_postgres_options = {
            'abc': 123
        }
        mock_postgres_copy_to_options.return_value = (
            mock_date_output_style,
            mock_date_order_style,
            mock_postgres_options,
        )
        mock_quote_value.return_value = "ABC"
        self.unloader.unload(mock_schema,
                             mock_table,
                             mock_unload_plan,
                             mock_directory)
        mock_processing_instructions = mock_unload_plan.processing_instructions
        mock_unhandled_hints = set(mock_records_format.hints.keys())
        mock_complain_on_unhandled_hints.\
            assert_called_with(mock_processing_instructions.fail_if_dont_understand,
                               mock_unhandled_hints,
                               mock_records_format.hints)
        mock_table_obj = mock_Table.return_value
        mock_Table.assert_called_with(mock_table,
                                      ANY,
                                      schema=mock_schema,
                                      autoload=True,
                                      autoload_with=self.mock_db)
        mock_conn = self.mock_db.engine.begin.return_value.__enter__.return_value
        mock_quote_value.assert_called_with(mock_conn, 'DATE_OUTPUT_STYLE, DATE_ORDER_STYLE')
        mock_conn.execute.assert_called_with('SET LOCAL DateStyle = ABC')
        mock_fileobj = mock_directory.loc.file_in_this_directory.return_value.open.\
            return_value.__enter__.return_value
        mock_copy_to.assert_called_with(mock_table_obj.select.return_value,
                                        mock_fileobj,
                                        mock_conn,
                                        abc=123)

    @patch('records_mover.db.postgres.unloader.quote_value')
    @patch('records_mover.db.postgres.unloader.copy_to')
    @patch('records_mover.db.postgres.unloader.complain_on_unhandled_hints')
    @patch('records_mover.db.postgres.unloader.Table')
    @patch('records_mover.db.postgres.unloader.postgres_copy_to_options')
    def test_unload_default_date_order_style(self,
                                             mock_postgres_copy_to_options,
                                             mock_Table,
                                             mock_complain_on_unhandled_hints,
                                             mock_copy_to,
                                             mock_quote_value):
        mock_schema = Mock(name='schema')
        mock_table = Mock(name='table')
        mock_unload_plan = Mock(name='unload_plan')
        mock_directory = MagicMock(name='directory')

        mock_records_format = Mock(name='records_format',
                                   spec=DelimitedRecordsFormat)
        mock_records_format.hints = {}
        mock_unload_plan.records_format = mock_records_format
        mock_date_output_style = "DATE_OUTPUT_STYLE"
        mock_date_order_style = None
        mock_postgres_options = {
            'abc': 123
        }
        mock_postgres_copy_to_options.return_value = (
            mock_date_output_style,
            mock_date_order_style,
            mock_postgres_options,
        )
        mock_quote_value.return_value = "ABC"
        self.unloader.unload(mock_schema,
                             mock_table,
                             mock_unload_plan,
                             mock_directory)
        mock_processing_instructions = mock_unload_plan.processing_instructions
        mock_unhandled_hints = set(mock_records_format.hints.keys())
        mock_complain_on_unhandled_hints.\
            assert_called_with(mock_processing_instructions.fail_if_dont_understand,
                               mock_unhandled_hints,
                               mock_records_format.hints)
        mock_table_obj = mock_Table.return_value
        mock_Table.assert_called_with(mock_table,
                                      ANY,
                                      schema=mock_schema,
                                      autoload=True,
                                      autoload_with=self.mock_db)
        mock_conn = self.mock_db.engine.begin.return_value.__enter__.return_value
        mock_quote_value.assert_called_with(mock_conn, 'DATE_OUTPUT_STYLE, MDY')
        mock_conn.execute.assert_called_with('SET LOCAL DateStyle = ABC')
        mock_fileobj = mock_directory.loc.file_in_this_directory.return_value.open.\
            return_value.__enter__.return_value
        mock_copy_to.assert_called_with(mock_table_obj.select.return_value,
                                        mock_fileobj,
                                        mock_conn,
                                        abc=123)

    @patch('records_mover.db.postgres.unloader.complain_on_unhandled_hints')
    @patch('records_mover.db.postgres.unloader.postgres_copy_to_options')
    def test_can_unload_this_format_true(self,
                                         mock_postgres_copy_to_options,
                                         mock_complain_on_unhandled_hints):
        source_records_format = Mock(name='source_records_format',
                                     spec=DelimitedRecordsFormat)
        source_records_format.hints = {}
        out = self.unloader.can_unload_this_format(source_records_format)
        self.assertTrue(out)
