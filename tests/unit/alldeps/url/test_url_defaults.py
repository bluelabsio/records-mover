from records_mover.url import init_urls
from records_mover import url
import unittest


class TestUrl(unittest.TestCase):
    def setUp(self):
        url.file_url_ctors = {}
        url.directory_url_ctors = {}

    def test_init_urls(self):
        init_urls()
        self.assertEqual(list(url.directory_url_ctors.keys()), ['s3', 'file'])
        self.assertEqual(list(url.file_url_ctors.keys()), ['s3', 'file', 'http', 'https'])
