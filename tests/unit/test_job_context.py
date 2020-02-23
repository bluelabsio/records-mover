from mock import patch, Mock
import unittest


class TestJobContext(unittest.TestCase):
    def mock_job_context(self):
        from records_mover import job_context

        return job_context.get_job_context('dummy_job')

    @patch('records_mover.base_job_context.engine_from_db_facts')
    def test_get_db_engine(self,
                           mock_engine_from_db_facts):
        mock_db_creds_name = Mock(name='db_creds_name')
        mock_creds = Mock(name='creds')
        job_context = self.mock_job_context()
        out = job_context.get_db_engine(mock_db_creds_name,
                                        creds_provider=mock_creds)
        mock_creds.db_facts.assert_called_with(mock_db_creds_name)
        mock_engine_from_db_facts.assert_called_with(mock_creds.db_facts.return_value)
        self.assertEquals(mock_engine_from_db_facts.return_value, out)
