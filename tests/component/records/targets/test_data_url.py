import unittest
from records_mover.url.filesystem import FilesystemFileUrl
from records_mover.records.targets.data_url import DataUrlTarget
from records_mover.records.records_format import DelimitedRecordsFormat


class TestDataUrlTarget(unittest.TestCase):
    def test_gzip_compression_inferred_no_records_format(self):
        url = FilesystemFileUrl('file:///foo/bar/baz.csv.gz')
        target = DataUrlTarget(url, records_format=None)
        self.assertEqual(target.records_format.hints['compression'], 'GZIP')

    def test_no_compression_inferred_no_records_format(self):
        url = FilesystemFileUrl('file:///foo/bar/baz.csv')
        target = DataUrlTarget(url, records_format=None)
        self.assertIsNone(target.records_format.hints['compression'])

    def test_no_compression_inferred_bluelabs_records_format(self):
        url = FilesystemFileUrl('file:///foo/bar/baz.csv')
        target = DataUrlTarget(url,
                               records_format=DelimitedRecordsFormat(variant='bluelabs'))
        self.assertIsNone(target.records_format.hints['compression'])

    def test_gzip_compression_inferred_bluelabs_records_format(self):
        url = FilesystemFileUrl('file:///foo/bar/baz.csv.gz')
        target = DataUrlTarget(url,
                               records_format=DelimitedRecordsFormat(variant='bluelabs'))
        self.assertEqual(target.records_format.hints['compression'], 'GZIP')

    def test_no_compression_not_inferred_customized_records_format(self):
        url = FilesystemFileUrl('file:///foo/bar/baz.csv')
        target = DataUrlTarget(url,
                               records_format=DelimitedRecordsFormat(variant='bluelabs',
                                                                     hints={'compression': 'GZIP'}))
        self.assertEqual(target.records_format.hints['compression'], 'GZIP')
