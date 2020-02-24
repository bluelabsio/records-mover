from mock import patch, Mock
import unittest


class TestSesssion(unittest.TestCase):
    def mock_session(self):
        from records_mover import session

        return session.Session()

    @patch('records_mover.session.engine_from_db_facts')
    def test_get_db_engine(self,
                           mock_engine_from_db_facts):
        mock_db_creds_name = Mock(name='db_creds_name')
        mock_creds = Mock(name='creds')
        session = self.mock_session()
        out = session.get_db_engine(mock_db_creds_name,
                                    creds_provider=mock_creds)
        mock_creds.db_facts.assert_called_with(mock_db_creds_name)
        mock_engine_from_db_facts.assert_called_with(mock_creds.db_facts.return_value)
        self.assertEquals(mock_engine_from_db_facts.return_value, out)
