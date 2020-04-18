import unittest
from unittest.mock import patch, Mock
from records_mover.db.postgres.copy_options import postgres_copy_to_options
from records_mover.records import DelimitedRecordsFormat


class TestPostgresCopyToOptions(unittest.TestCase):
    @patch('records_mover.db.postgres.copy_options.postgres_copy_options_csv')
    @patch('records_mover.db.postgres.copy_options.postgres_copy_options_text')
    @patch('records_mover.db.postgres.copy_options.determine_output_date_order_style')
    def test_postgres_copy_to_options_csv_type(self,
                                               mock_determine_output_date_order_style,
                                               mock_postgres_copy_options_text,
                                               mock_postgres_copy_options_csv):
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={'header-row': True})
        mock_date_output_style = Mock(name='date_output_style')
        mock_date_order_style = Mock(name='date_order_style')
        mock_postgres_copy_options = mock_postgres_copy_options_csv.return_value
        mock_determine_output_date_order_style.return_value = (mock_date_output_style,
                                                               mock_date_order_style)
        unhandled_hints = set()
        out = postgres_copy_to_options(unhandled_hints,
                                       records_format,
                                       fail_if_cant_handle_hint=True)
        self.assertEqual(out,
                         (mock_date_output_style,
                          mock_date_order_style,
                          mock_postgres_copy_options))
        # Header row requires CSV type
        mock_postgres_copy_options_csv.assert_called()

    @patch('records_mover.db.postgres.copy_options.postgres_copy_options_csv')
    @patch('records_mover.db.postgres.copy_options.postgres_copy_options_text')
    @patch('records_mover.db.postgres.copy_options.determine_output_date_order_style')
    def test_postgres_copy_to_options_text_type(self,
                                                mock_determine_output_date_order_style,
                                                mock_postgres_copy_options_text,
                                                mock_postgres_copy_options_csv):
        records_format = DelimitedRecordsFormat(variant='bluelabs')
        mock_date_output_style = Mock(name='date_output_style')
        mock_date_order_style = Mock(name='date_order_style')
        mock_postgres_copy_options = mock_postgres_copy_options_text.return_value
        mock_determine_output_date_order_style.return_value = (mock_date_output_style,
                                                               mock_date_order_style)
        unhandled_hints = set()
        out = postgres_copy_to_options(unhandled_hints,
                                       records_format,
                                       fail_if_cant_handle_hint=True)
        self.assertEqual(out,
                         (mock_date_output_style,
                          mock_date_order_style,
                          mock_postgres_copy_options))
        # Header row requires text type
        mock_postgres_copy_options_text.assert_called()
