from records_mover import Session
import unittest
from mock import patch


@patch.dict('os.environ', {
    'AWS_SECRET_ACCESS_KEY': 'aws secret key',
    'AWS_SESSION_TOKEN': 'aws session token',
    'AWS_ACCESS_KEY_ID': 'aws access key',
})
class TestCLISesssionEngine(unittest.TestCase):
    @patch('records_mover.session.db_engine')
    def test_get_default_db_engine(self, mock_db_engine):
        context = Session(session_type='cli',
                                  default_db_creds_name=None,
                                  default_aws_creds_name=None)
        self.assertEqual(mock_db_engine.return_value, context.get_default_db_engine())
        mock_db_engine.assert_called_with(context)
