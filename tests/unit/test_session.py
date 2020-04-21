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

    @patch('records_mover.session.CredsViaEnv')
    def test_itest_type_uses_creds_via_env(self,
                                           mock_CredsViaEnv,
                                           mock_os,
                                           mock_subprocess):
        mock_creds = mock_CredsViaEnv.return_value
        session = Session(session_type='itest')
        self.assertEqual(session.creds, mock_creds)

    @patch('records_mover.session.CredsViaEnv')
    def test_env_type_uses_creds_via_env(self,
                                         mock_CredsViaEnv,
                                         mock_os,
                                         mock_subprocess):
        mock_creds = mock_CredsViaEnv.return_value
        session = Session(session_type='env')
        self.assertEqual(session.creds, mock_creds)

    @patch('records_mover.session.db_facts_from_env')
    @patch('records_mover.db.connect.engine_from_db_facts')
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

    @patch('records_mover.session.db_facts_from_env')
    @patch('records_mover.db.connect.engine_from_db_facts')
    def test_get_default_db_facts_no_default(self,
                                             mock_engine_from_db_facts,
                                             mock_db_facts_from_env,
                                             mock_os,
                                             mock_subprocess):
        session = Session()
        self.assertEqual(session.get_default_db_facts(), mock_db_facts_from_env.return_value)
        mock_db_facts_from_env.assert_called_with()

    @patch('records_mover.db.connect.engine_from_db_facts')
    def test_get_default_db_facts_with_default(self,
                                               mock_engine_from_db_facts,
                                               mock_os,
                                               mock_subprocess):
        mock_creds = Mock(name='creds')
        mock_default_db_creds_name = Mock(name='default_db_creds_name')
        session = Session(creds=mock_creds,
                          default_db_creds_name=mock_default_db_creds_name)
        self.assertEqual(session.get_default_db_facts(),
                         mock_creds.db_facts.return_value)
        mock_creds.db_facts.assert_called_with(mock_default_db_creds_name)

    @patch('records_mover.session.set_stream_logging')
    def test_set_stream_logging(self,
                                mock_set_stream_logging,
                                mock_os,
                                mock_subprocess):
        session = Session()
        mock_name = Mock(name='name')
        mock_level = Mock(name='level')
        mock_stream = Mock(name='stream')
        mock_fmt = Mock(name='fmt')
        mock_datefmt = Mock(name='datefmt')
        session.set_stream_logging(name=mock_name,
                                   level=mock_level,
                                   stream=mock_stream,
                                   fmt=mock_fmt,
                                   datefmt=mock_datefmt)
        mock_set_stream_logging.assert_called_with(name=mock_name,
                                                   level=mock_level,
                                                   stream=mock_stream,
                                                   fmt=mock_fmt,
                                                   datefmt=mock_datefmt)
