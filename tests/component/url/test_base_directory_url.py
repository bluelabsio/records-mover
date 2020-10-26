from records_mover.url.base import BaseDirectoryUrl
import unittest
from unittest.mock import Mock


class TestBaseDirectoryUrl(unittest.TestCase):
    def test_files_and_directories_in_directory(self):
        mock_file_1 = Mock(name='file_1')
        mock_file_2 = Mock(name='file_2')
        mock_dir_1 = Mock(name='dir_1')
        mock_dir_2 = Mock(name='dir_2')

        mock_url = Mock(name='url')

        class MockDirectoryUrl(BaseDirectoryUrl):
            def __init__(self, url):
                pass

            def files_in_directory(self):
                return [mock_file_1, mock_file_2]

            def directories_in_directory(self):
                return [mock_dir_1, mock_dir_2]

        mock_directory_url = MockDirectoryUrl(mock_url)
        self.assertEqual(set(mock_directory_url.files_and_directories_in_directory()),
                         {mock_file_1, mock_file_2, mock_dir_1, mock_dir_2})
