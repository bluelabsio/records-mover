from records_mover.url import FileUrl, DirectoryUrl, directory_url_ctors, file_url_ctors
from records_mover.url.base import BaseFileUrl, BaseDirectoryUrl
from mock import Mock
import unittest


class TestUrl(unittest.TestCase):
    def setUp(self):
        self.mock_DummyFileUrl = Mock(name='DummyFileUrl', spec=BaseFileUrl)
        self.mock_DummyFileUrl.return_value = Mock(name='dummyfileurl',
                                                        spec=BaseFileUrl)
        self.mock_DummyDirectoryUrl = Mock(name='DummyDirectoryUrl')
        self.mock_DummyDirectoryUrl.return_value = Mock(name='dummydirectoryurl',
                                                        spec=BaseDirectoryUrl)
        file_url_ctors['dummy'] = self.mock_DummyFileUrl
        directory_url_ctors['dummy'] = self.mock_DummyDirectoryUrl

    def test_FileUrl(self):
        dummy_url = 'dummy://foo/bar/baz?a=b&d=f'
        file_url = FileUrl(dummy_url, mock_arg='123')
        self.mock_DummyFileUrl.assert_called_with('dummy://foo/bar/baz?a=b&d=f',
                                                  mock_arg='123')
        self.assertEqual(self.mock_DummyFileUrl.return_value, file_url)

    def test_DirectoryUrl(self):
        dummy_url = 'dummy://foo/bar/?a=b&d=f'
        directory_url = DirectoryUrl(dummy_url, mock_arg=123)
        self.mock_DummyDirectoryUrl.assert_called_with('dummy://foo/bar/?a=b&d=f',
                                                       mock_arg=123)
        self.assertEqual(self.mock_DummyDirectoryUrl.return_value, directory_url)
