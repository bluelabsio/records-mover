from records_mover.utils import beta, BetaWarning
import unittest
from unittest.mock import patch


@patch('records_mover.utils.warnings')
class TestBeta(unittest.TestCase):
    def test_beta(self, mock_warnings):
        @beta
        def my_crazy_function():
            return 123

        out = my_crazy_function()
        self.assertEqual(out, 123)
        mock_warnings.warn.assert_called_with('Call to beta function my_crazy_function - '
                                              'interface may change in the future!',
                                              category=BetaWarning,
                                              stacklevel=2)
