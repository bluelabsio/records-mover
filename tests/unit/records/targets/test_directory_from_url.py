import unittest
from records_mover.records.targets.directory_from_url import DirectoryFromUrlRecordsTarget
from mock import Mock


class TestDirectoryFromUrlRecordsTarget(unittest.TestCase):
    def test_init(self):
        output_url = 'mumble://foo/bar/baz/'
        mock_url_resolver = Mock(name='url_resolver')
        mock_directory_url = mock_url_resolver.directory_url
        mock_records_format = Mock(name='records_format')
        target = DirectoryFromUrlRecordsTarget(output_url=output_url,
                                               url_resolver=mock_url_resolver,
                                               records_format=mock_records_format)
        self.assertEqual(target.records_format, mock_records_format)
        mock_directory_url.assert_called_with(output_url)
        self.assertEqual(target.output_loc, mock_directory_url.return_value)

    def test_known_supported_records_formats_one(self):
        output_url = 'mumble://foo/bar/baz/'
        mock_url_resolver = Mock(name='url_resolver')
        mock_records_format = Mock(name='records_format')
        target = DirectoryFromUrlRecordsTarget(output_url=output_url,
                                               url_resolver=mock_url_resolver,
                                               records_format=mock_records_format)
        out = target.known_supported_records_formats()
        self.assertEqual(out, [mock_records_format])

    def test_known_supported_records_formats_zero(self):
        output_url = 'mumble://foo/bar/baz/'
        mock_url_resolver = Mock(name='url_resolver')
        mock_records_format = None
        target = DirectoryFromUrlRecordsTarget(output_url=output_url,
                                               url_resolver=mock_url_resolver,
                                               records_format=mock_records_format)
        out = target.known_supported_records_formats()
        self.assertEqual(out, [])

    def test_can_move_from_this_format_always_when_None(self):
        output_url = 'mumble://foo/bar/baz/'
        mock_url_resolver = Mock(name='url_resolver')
        mock_records_format = None
        mock_rando_records_format = Mock(name='rando_records_format')
        target = DirectoryFromUrlRecordsTarget(output_url=output_url,
                                               url_resolver=mock_url_resolver,
                                               records_format=mock_records_format)
        out = target.can_move_from_this_format(mock_rando_records_format)
        self.assertEqual(out, True)
