from records_mover.url.http import HttpFileUrl
from mock import patch, Mock
import unittest


class TestHttpFileUrl(unittest.TestCase):
    def setUp(self):
        self.http_file_url = HttpFileUrl('http://site.com/path/file#foo?a=b&b=c')

    def test_url(self):
        self.assertEqual(self.http_file_url.url, 'http://site.com/path/file#foo?a=b&b=c')

    @patch("records_mover.url.urllib.urlopen")
    @patch("records_mover.url.base.blcopyfileobj")
    def test_download_fileobj(self, mock_copyfileobj, mock_urlopen):
        mock_output_fileobj = Mock(name='output_fileobj')
        self.http_file_url.download_fileobj(mock_output_fileobj)
        mock_urlopen.assert_called_with('http://site.com/path/file#foo?a=b&b=c')
        mock_copyfileobj.assert_called_with(mock_urlopen.return_value.__enter__.return_value,
                                            mock_output_fileobj)
