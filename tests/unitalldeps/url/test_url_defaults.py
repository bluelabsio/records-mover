from records_mover.url import init_urls
from records_mover import url
import os
import unittest


# def skipIfTrue(flag):
#     def deco(f):
#         def wrapper(self, *args, **kwargs):
#             if getattr(self, flag):
#                 self.skipTest()
#             else:
#                 f(self, *args, **kwargs)
#         return wrapper
#     return deco


class TestUrl(unittest.TestCase):
    def setUp(self):
        url.file_url_ctors = {}
        url.directory_url_ctors = {}

    @unittest.skipIf(os.environ.get('RECORDS_MOVER_UNIT_TEST_NO_AWS') is not None, 'No AWS')
    def test_init_urls_with_s3(self):
        init_urls()
        self.assertEqual(list(url.directory_url_ctors.keys()), ['s3', 'file'])
        self.assertEqual(list(url.file_url_ctors.keys()), ['s3', 'file', 'http', 'https'])

    @unittest.skipIf(os.environ.get('RECORDS_MOVER_UNIT_TEST_NO_AWS') is None, 'No AWS')
    def test_init_urls_with_no_s3(self):
        init_urls()
        self.assertEqual(list(url.directory_url_ctors.keys()), ['file'])
        self.assertEqual(list(url.file_url_ctors.keys()), ['file', 'http', 'https'])
