from records_mover.records.sources.uninferred_fileobjs import UninferredFileobjsRecordsSource
from mock import Mock
import unittest


class TestUninferredFileobjsFileobjsSource(unittest.TestCase):
    def test_init(self):
        mock_fileobj = Mock(name='fileobj')
        mock_target_names_to_input_fileobjs = {
            'foo': mock_fileobj
        }
        out = UninferredFileobjsRecordsSource(mock_target_names_to_input_fileobjs)
        self.assertIsNotNone(out)
