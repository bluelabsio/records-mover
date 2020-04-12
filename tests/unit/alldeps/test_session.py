from mock import patch, Mock
from records_mover import Session
import unittest


@patch('records_mover.session.subprocess')
@patch('records_mover.session.os')
class TestSession(unittest.TestCase):
    @patch('records_mover.db.connect.engine_from_db_facts')
    def test_get_db_engine(self,
                           mock_engine_from_db_facts,
                           mock_os,
                           mock_subprocess):
        mock_db_creds_name = Mock(name='db_creds_name')
        mock_creds = Mock(name='creds')
        session = Session()
        out = session.get_db_engine(mock_db_creds_name,
                                    creds_provider=mock_creds)
        mock_creds.db_facts.assert_called_with(mock_db_creds_name)
        mock_engine_from_db_facts.assert_called_with(mock_creds.db_facts.return_value)
        self.assertEqual(mock_engine_from_db_facts.return_value, out)

    @patch('records_mover.session.db_facts_from_env')
    @patch('records_mover.session.engine_from_db_facts')
    def test_get_default_db_engine_no_default(self,
                                              mock_engine_from_db_facts,
                                              mock_db_facts_from_env,
                                              mock_os,
                                              mock_subprocess):
        session = Session()
        self.assertEqual(session.get_default_db_engine(), mock_engine_from_db_facts.return_value)
        mock_db_facts_from_env.assert_called_with()
        mock_db_facts = mock_db_facts_from_env.return_value
        mock_engine_from_db_facts.assert_called_with(mock_db_facts)
