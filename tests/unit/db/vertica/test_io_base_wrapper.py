import unittest
from mock import Mock
from records_mover.db.vertica.io_base_wrapper import IOBaseWrapper


class TestIOBaseWrapper(unittest.TestCase):
    def test_read(self):
        mock_obj = Mock(name='obj')
        wrapper = IOBaseWrapper(mock_obj)
        out = wrapper.read()
        self.assertEqual(out, mock_obj.read.return_value)
