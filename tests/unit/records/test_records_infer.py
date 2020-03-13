import unittest
from records_mover.records.records import Records
from mock import patch


class TestRecordsInfer(unittest.TestCase):
    @patch('records_mover.Session')
    @patch('records_mover.records.records.Records')
    @patch('records_mover.records.records.RecordsSources')
    @patch('records_mover.records.records.RecordsTargets')
    def test_record_infer(self,
                          mock_RecordsTargets,
                          mock_RecordsSources,
                          mock_Records,
                          mock_Session):
        mock_session = mock_Session.return_value
        records = Records()
        self.assertEqual(records.sources, mock_RecordsSources.return_value)
        self.assertEqual(records.targets, mock_RecordsTargets.return_value)
        mock_RecordsTargets.assert_called_with(db_driver=mock_session.db_driver,
                                               url_resolver=mock_session.url_resolver)
        mock_RecordsSources.assert_called_with(db_driver=mock_session.db_driver,
                                               url_resolver=mock_session.url_resolver)
