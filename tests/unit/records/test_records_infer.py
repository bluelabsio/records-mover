import unittest
from records_mover.records.records import Records
from mock import patch


class TestRecordsInfer(unittest.TestCase):
    @patch('records_mover.Session')
    @patch('records_mover.records.records.Records')
    def test_record_infer(self, mock_Records, mock_Session):
        mock_session = mock_Session.return_value
        records = Records()
        self.assertEqual(records.db_driver, mock_session.db_driver)
        self.assertEqual(records.url_resolver, mock_session.url_resolver)
