from records_mover.utils import deprecated
import unittest
from unittest.mock import patch


@patch('records_mover.utils.warnings')
class TestDeprecated(unittest.TestCase):
    def test_deprecated(self, mock_warnings):
        @deprecated
        def my_crazy_function():
            return 123

        out = my_crazy_function()
        self.assertEqual(out, 123)
        mock_warnings.warn.assert_called_with('Call to deprecated function my_crazy_function.',
                                              category=DeprecationWarning,
                                              stacklevel=2)
