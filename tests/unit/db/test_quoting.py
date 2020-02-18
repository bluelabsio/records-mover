import unittest
from unittest.mock import Mock, call
from records_mover.db import quoting


class TestQuoting(unittest.TestCase):
    def test_quote_table_only(self):
        mock_engine = Mock()
        quotable_value = Mock()
        mock_preparer = mock_engine.dialect.preparer.return_value
        quoted = quoting.quote_table_only(mock_engine, quotable_value)
        self.assertEqual(mock_preparer.quote.return_value, quoted)
        mock_preparer.quote.assert_called_with(quotable_value)

    def test_quote_column_name(self):
        mock_engine = Mock()
        quotable_value = Mock()
        mock_preparer = mock_engine.dialect.preparer.return_value
        quoted = quoting.quote_column_name(mock_engine, quotable_value)
        self.assertEqual(mock_preparer.quote.return_value, quoted)
        mock_preparer.quote.assert_called_with(quotable_value)

    def test_quote_schema_and_table(self):
        mock_engine = Mock()
        quotable_table = Mock()
        quotable_schema = Mock()
        mock_preparer = mock_engine.dialect.preparer.return_value
        mock_preparer.quote.return_value = '"foo"'
        quoted = quoting.quote_schema_and_table(mock_engine,
                                                quotable_schema,
                                                quotable_table)
        self.assertEqual(mock_preparer.quote.return_value + "." +
                         mock_preparer.quote.return_value,
                         quoted)
        mock_preparer.quote.assert_has_calls([call(quotable_schema),
                                              call(quotable_table)])

    def test_quote_value(self):
        mock_engine = Mock()
        quotable_value = Mock()
        mock_preparer = mock_engine.dialect.preparer.return_value
        quoted = quoting.quote_value(mock_engine, quotable_value)
        self.assertEqual(mock_preparer.quote.return_value, quoted)
        mock_preparer.quote.assert_called_with(quotable_value)
        mock_engine.dialect.preparer.assert_called_with(mock_engine.dialect,
                                                        initial_quote="'")
