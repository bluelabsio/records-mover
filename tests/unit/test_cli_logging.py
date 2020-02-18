from records_mover.cli import cli_logging
import unittest
from mock import patch


@patch('records_mover.cli.cli_logging.stdout_handler')
@patch('records_mover.cli.cli_logging.logging')
class TestCLILogging(unittest.TestCase):
    def test_basic_config(self, mock_logging, mock_stdout_handler):
        cli_logging.basic_config()

        mock_root_logger = mock_logging.getLogger.return_value
        mock_logging.getLogger.assert_called_with()
        mock_root_logger.addHandler.assert_called_with(mock_stdout_handler)
