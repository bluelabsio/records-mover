from records_mover.url.optimizer.awscli import aws_cli
from mock import patch, call
import unittest


class TestAwsCli(unittest.TestCase):
    @patch("records_mover.url.s3.awscli.dict")
    @patch("records_mover.url.s3.awscli.os")
    @patch("records_mover.url.s3.awscli.create_clidriver")
    def test_aws_cli(self,
                     mock_create_cli_driver,
                     mock_os,
                     mock_dict):
        mock_cli_driver = mock_create_cli_driver.return_value
        mock_cli_driver.main.return_value = 0
        mock_os.environ.copy.return_value = {
            'old_value': 'old',
            'LC_CTYPE': 'orig_LC_CTYPE'
        }
        aws_cli('a', 'b', 'c')
        mock_dict.assert_called_with(mock_os.environ)
        mock_cli_driver.main.assert_called_with(args=('a', 'b', 'c'))
        mock_os.environ.update.assert_has_calls([
            call({'old_value': 'old', 'LC_CTYPE': 'en_US.UTF'}),
            call(mock_dict.return_value)
        ])
        mock_os.environ.clear.assert_called_with()
