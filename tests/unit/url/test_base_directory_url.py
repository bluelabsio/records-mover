import unittest
from unittest.mock import Mock, patch
from records_mover.url.base import BaseDirectoryUrl


class TestBaseDirectoryUrl(unittest.TestCase):
    def test_empty_true(self):
        mock_url = Mock(name='url')

        class MockDirectoryUrl(BaseDirectoryUrl):
            def __init__(self, url):
                pass

            def directories_in_directory(self):
                return []

            def files_in_directory(self):
                return []

        mock_directory_loc = MockDirectoryUrl(url=mock_url)
        self.assertTrue(mock_directory_loc.empty())

    def test_size_empty(self):
        mock_url = Mock(name='url')

        class MockDirectoryUrl(BaseDirectoryUrl):
            def __init__(self, url):
                pass

            def directories_in_directory(self):
                return []

            def files_in_directory(self):
                return []

        mock_directory_loc = MockDirectoryUrl(url=mock_url)
        self.assertEqual(mock_directory_loc.size(), 0)

    def test_size_one_file(self):
        mock_url = Mock(name='url')
        mock_file = Mock(name='file')
        mock_file.size.return_value = 123

        class MockDirectoryUrl(BaseDirectoryUrl):
            def __init__(self, url):
                pass

            def directories_in_directory(self):
                return []

            def files_in_directory(self):
                return [mock_file]

        mock_directory_loc = MockDirectoryUrl(url=mock_url)
        self.assertEqual(mock_directory_loc.size(), 123)

    @patch('records_mover.url.base.secrets')
    def test_temporary_file(self, mock_secrets):
        mock_url = Mock(name='url')
        mock_file = Mock(name='file')
        mock_random_slug = mock_secrets.token_urlsafe.return_value

        class MockDirectoryUrl(BaseDirectoryUrl):
            def __init__(self, url):
                pass

            def file_in_this_directory(self, path):
                assert path == mock_random_slug
                return mock_file

            def files_in_directory(self):
                return [mock_file]

        mock_directory_loc = MockDirectoryUrl(url=mock_url)
        mock_file.exists.return_value = True

        with mock_directory_loc.temporary_file() as temp_loc:
            self.assertEqual(temp_loc, mock_file)

        mock_file.exists.assert_called_with()
        mock_file.delete.assert_called_with()
        mock_secrets.token_urlsafe.assert_called_with(8)

    @patch('records_mover.url.base.secrets')
    def test_writable_true(self, mock_secrets):
        mock_url = Mock(name='url')
        mock_file = Mock(name='file')
        mock_random_slug = mock_secrets.token_urlsafe.return_value

        class MockDirectoryUrl(BaseDirectoryUrl):
            def __init__(self, url):
                pass

            def file_in_this_directory(self, path):
                assert path == mock_random_slug
                return mock_file

            def files_in_directory(self):
                return [mock_file]

        mock_directory_loc = MockDirectoryUrl(url=mock_url)
        mock_file.exists.return_value = True

        self.assertTrue(mock_directory_loc.writable())

        mock_file.exists.assert_called_with()
        mock_file.delete.assert_called_with()
        mock_secrets.token_urlsafe.assert_called_with(8)

    @patch('records_mover.url.base.secrets')
    def test_writable_false(self, mock_secrets):
        mock_url = 'whatever'
        mock_file = Mock(name='file')
        mock_random_slug = mock_secrets.token_urlsafe.return_value

        class MockDirectoryUrl(BaseDirectoryUrl):
            def __init__(self, url):
                self.url = url

            def file_in_this_directory(self, path):
                assert path == mock_random_slug
                return mock_file

            def files_in_directory(self):
                return [mock_file]

        mock_directory_loc = MockDirectoryUrl(url=mock_url)
        mock_file.exists.return_value = True
        mock_file.store_string.side_effect = Exception

        self.assertFalse(mock_directory_loc.writable())

        mock_file.exists.assert_called_with()
        mock_file.delete.assert_called_with()
        mock_secrets.token_urlsafe.assert_called_with(8)

    @patch('records_mover.url.base.secrets')
    def test_is_directory_true(self, mock_secrets):
        mock_url = 'whatever://foo/'

        class MockDirectoryUrl(BaseDirectoryUrl):
            def __init__(self, url):
                self.url = url

        mock_directory_loc = MockDirectoryUrl(url=mock_url)

        self.assertTrue(mock_directory_loc.is_directory())
