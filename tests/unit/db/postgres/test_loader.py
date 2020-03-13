import unittest
from records_mover.db.postgres.loader import PostgresLoader
from records_mover.records import DelimitedRecordsFormat
from mock import MagicMock, Mock, patch
import sqlalchemy


class TestPostgresLoader(unittest.TestCase):
    @patch('records_mover.db.postgres.loader.quote_value')
    @patch('records_mover.db.postgres.loader.copy_from')
    @patch('records_mover.db.postgres.loader.complain_on_unhandled_hints')
    @patch('records_mover.db.postgres.loader.Table')
    @patch('records_mover.db.postgres.loader.postgres_copy_options')
    def test_load_from_fileobj(self,
                               mock_postgres_copy_options,
                               mock_Table,
                               mock_complain_on_unhandled_hints,
                               mock_copy_from,
                               mock_quote_value):
        mock_url_resolver = Mock(name='url_resolver')
        mock_meta = Mock(name='meta')
        mock_db = MagicMock(name='db')
        loader = PostgresLoader(mock_url_resolver,
                                mock_meta,
                                mock_db)
        mock_schema = Mock(name='schema')
        mock_table = Mock(name='table')
        mock_load_plan = Mock(name='load_plan')
        mock_fileobj = Mock(name='fileobj')

        mock_records_format = Mock(name='records_format',
                                   spec=DelimitedRecordsFormat)
        mock_records_format.hints = {}
        mock_load_plan.records_format = mock_records_format
        mock_date_input_style = "DATE_INPUT_STYLE"
        mock_postgres_options = {
            'abc': 123
        }
        mock_postgres_copy_options.return_value = (
            mock_date_input_style,
            mock_postgres_options,
        )
        mock_quote_value.return_value = "ABC"
        loader.load_from_fileobj(mock_schema,
                                 mock_table,
                                 mock_load_plan,
                                 mock_fileobj)
        mock_processing_instructions = mock_load_plan.processing_instructions
        mock_unhandled_hints = set(mock_records_format.hints.keys())
        mock_complain_on_unhandled_hints.\
            assert_called_with(mock_processing_instructions.fail_if_dont_understand,
                               mock_unhandled_hints,
                               mock_records_format.hints)
        mock_table_obj = mock_Table.return_value
        mock_Table.assert_called_with(mock_table,
                                      mock_meta,
                                      schema=mock_schema,
                                      autoload=True,
                                      autoload_with=mock_db)
        mock_conn = mock_db.engine.begin.return_value.__enter__.return_value
        mock_quote_value.assert_called_with(mock_conn, 'ISO, DATE_INPUT_STYLE')
        mock_conn.execute.assert_called_with('SET LOCAL DateStyle = ABC')
        mock_copy_from.assert_called_with(mock_fileobj,
                                          mock_table_obj,
                                          mock_conn,
                                          abc=123)
