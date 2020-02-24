import unittest
from mock import patch, Mock
from records_mover.records.csv_streamer import stream_csv


@patch('pandas.read_csv')
@patch('pandas.io.parsers.TextFileReader')
@patch('records_mover.records.csv_streamer.io')
class TestCsvStreamer(unittest.TestCase):
    def test_stream_csv_no_compression(self,
                                       mock_io,
                                       mock_TextFileReader,
                                       mock_read_csv):
        mock_field_delimiter = Mock(name='field_delimiter')
        mock_hints = {
            'quoting': 'minimal',
            'field-delimiter': mock_field_delimiter,
        }
        mock_filepath_or_buffer = Mock(name='filepath_or_buffer')
        with stream_csv(mock_filepath_or_buffer, mock_hints) as out:
            mock_read_csv.assert_called_with(mock_io.TextIOWrapper.return_value,
                                             compression=None,
                                             encoding='utf-8',
                                             engine='python',
                                             escapechar=None,
                                             header='infer',
                                             prefix='untitled_',
                                             quoting=0,
                                             iterator=True,
                                             sep=mock_field_delimiter)
            mock_io.TextIOWrapper.assert_called_with(mock_filepath_or_buffer, encoding=None)
            self.assertEqual(out, mock_read_csv.return_value)
        mock_read_csv.return_value.close.assert_called()

    def test_stream_csv_gzip(self,
                             mock_io,
                             mock_TextFileReader,
                             mock_read_csv):
        mock_field_delimiter = Mock(name='field_delimiter')
        mock_hints = {
            'field-delimiter': mock_field_delimiter,
            'compression': 'GZIP',
        }
        mock_filepath_or_buffer = Mock(name='filepath_or_buffer')
        with stream_csv(mock_filepath_or_buffer, mock_hints) as out:
            mock_read_csv.assert_called_with(mock_filepath_or_buffer,
                                             compression='gzip',
                                             encoding='utf-8',
                                             engine='python',
                                             escapechar=None,
                                             header='infer',
                                             prefix='untitled_',
                                             iterator=True,
                                             sep=mock_field_delimiter)
            mock_io.TextIOWrapper.assert_not_called()
            self.assertEqual(out, mock_read_csv.return_value)
        mock_read_csv.return_value.close.assert_called()

    def test_stream_filename(self,
                             mock_io,
                             mock_TextFileReader,
                             mock_read_csv):
        mock_field_delimiter = Mock(name='field_delimiter')
        mock_hints = {
            'field-delimiter': mock_field_delimiter,
        }
        mock_filepath_or_buffer = 'my_filename'
        with stream_csv(mock_filepath_or_buffer, mock_hints) as out:
            mock_read_csv.assert_called_with(mock_filepath_or_buffer,
                                             compression=None,
                                             encoding='utf-8',
                                             engine='python',
                                             escapechar=None,
                                             header='infer',
                                             prefix='untitled_',
                                             iterator=True,
                                             sep=mock_field_delimiter)
            mock_io.TextIOWrapper.assert_not_called()
            self.assertEqual(out, mock_read_csv.return_value)
        mock_read_csv.return_value.close.assert_called()
