import unittest
from unittest.mock import Mock, MagicMock
from typing import IO, Any
from records_mover.url.base import BaseFileUrl


class TestBaseFileUrl(unittest.TestCase):
    def test_copy_to(self):
        mock_fileobj = MagicMock(name='fileobj')

        class MockFileUrl(BaseFileUrl):
            def __init__(self, url):
                pass

            def open(self, mode: str = "rb") -> IO[Any]:
                return mock_fileobj

        mock_url = Mock(name='url')
        mock_file_loc = MockFileUrl(mock_url)
        mock_other_loc = Mock(name='other_loc')
        out = mock_file_loc.copy_to(mock_other_loc)
        mock_other_loc.upload_fileobj.assert_called_with(mock_fileobj.__enter__.return_value)
        self.assertEqual(out, mock_other_loc)
