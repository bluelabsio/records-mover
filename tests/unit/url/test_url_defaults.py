from records_mover.url.resolver import init_urls
from records_mover.url import resolver
import unittest


class TestUrl(unittest.TestCase):
    def setUp(self):
        resolver.file_url_ctors = {}
        resolver.directory_url_ctors = {}

    def test_init_urls(self):
        init_urls()
        self.assertEqual(list(resolver.directory_url_ctors.keys()),
                         ['s3', 'gs', 'file'])
        self.assertEqual(list(resolver.file_url_ctors.keys()),
                         ['s3', 'gs', 'file', 'http', 'https'])
