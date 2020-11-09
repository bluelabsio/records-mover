from records_mover.records.sources.fileobjs import FileobjsSource
from records_mover.records.records_format import DelimitedRecordsFormat
from mock import Mock, patch
import unittest


class TestFileobjsSource(unittest.TestCase):
    @patch('records_mover.records.sources.fileobjs.sniff_hints_from_fileobjs')
    def test_infer_if_needed(self,
                             mock_sniff_hints_from_fileobjs):
        mock_fileobj = Mock(name='fileobj')
        mock_target_names_to_input_fileobjs = {
            'foo': mock_fileobj
        }
        mock_records_schema = Mock(name='records_schema')
        mock_processing_instructions = Mock(name='processing_instructions')
        with FileobjsSource.\
                infer_if_needed(target_names_to_input_fileobjs=mock_target_names_to_input_fileobjs,
                                processing_instructions=mock_processing_instructions,
                                records_schema=mock_records_schema,
                                records_format=None,
                                initial_hints={}):
            mock_sniff_hints_from_fileobjs.assert_called_with([mock_fileobj], initial_hints={})

    @patch('records_mover.records.sources.fileobjs.sniff_hints_from_fileobjs')
    def test_infer_if_needed_no_format_no_initial_hints(self,
                                                        mock_sniff_hints_from_fileobjs):
        mock_fileobj = Mock(name='fileobj')
        mock_target_names_to_input_fileobjs = {
            'foo': mock_fileobj
        }
        mock_records_schema = Mock(name='records_schema')
        mock_processing_instructions = Mock(name='processing_instructions')
        with FileobjsSource.\
                infer_if_needed(target_names_to_input_fileobjs=mock_target_names_to_input_fileobjs,
                                processing_instructions=mock_processing_instructions,
                                records_schema=mock_records_schema,
                                records_format=None,
                                initial_hints=None):
            mock_sniff_hints_from_fileobjs.assert_called_with([mock_fileobj], initial_hints={})

    @patch('records_mover.records.sources.fileobjs.RecordsSchema')
    @patch('records_mover.records.sources.fileobjs.DelimitedRecordsFormat')
    @patch('records_mover.records.sources.fileobjs.sniff_hints_from_fileobjs')
    def test_infer_if_needed_no_format_no_schema_no_initial_hints(self,
                                                                  mock_sniff_hints_from_fileobjs,
                                                                  mock_DelimitedRecordsFormat,
                                                                  mock_RecordsSchema):
        mock_fileobj = Mock(name='fileobj')
        mock_target_names_to_input_fileobjs = {
            'foo': mock_fileobj
        }
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_records_format = mock_DelimitedRecordsFormat.return_value
        mock_records_schema = mock_RecordsSchema.from_fileobjs.return_value
        with FileobjsSource.\
                infer_if_needed(target_names_to_input_fileobjs=mock_target_names_to_input_fileobjs,
                                processing_instructions=mock_processing_instructions,
                                records_schema=None,
                                records_format=None,
                                initial_hints=None) as out:
            mock_sniff_hints_from_fileobjs.assert_called_with([mock_fileobj], initial_hints={})
            self.assertEqual(out.records_format, mock_records_format)
            self.assertEqual(out.records_schema, mock_records_schema)
        mock_RecordsSchema.from_fileobjs.\
            assert_called_with([mock_fileobj],
                               records_format=mock_records_format,
                               processing_instructions=mock_processing_instructions)

    @patch('records_mover.records.sources.fileobjs.sniff_hints_from_fileobjs')
    def test_infer_if_needed_bad_encoding(self,
                                          mock_sniff_hints_from_fileobjs):
        mock_fileobj = Mock(name='fileobj')
        mock_target_names_to_input_fileobjs = {
            'foo': mock_fileobj
        }
        mock_records_schema = Mock(name='records_schema')
        mock_sniff_hints_from_fileobjs.side_effect = UnicodeDecodeError('foo',
                                                                        b'bar',
                                                                        1, 2,
                                                                        'reason')
        mock_processing_instructions = Mock(name='processing_instructions')
        with self.assertRaises(TypeError):
            with FileobjsSource.\
                 infer_if_needed(target_names_to_input_fileobjs=mock_target_names_to_input_fileobjs,
                                 processing_instructions=mock_processing_instructions,
                                 records_schema=mock_records_schema,
                                 records_format=None,
                                 initial_hints={}):
                pass

    def test_known_supported_records_formats(self):
        mock_records_format = Mock(name='records_format')
        mock_records_schema = Mock(name='records_schema')
        mock_target_names_to_input_fileobjs = Mock(name='target_names_to_input_fileobjs')
        source = FileobjsSource(target_names_to_input_fileobjs=mock_target_names_to_input_fileobjs,
                                records_schema=mock_records_schema,
                                records_format=mock_records_format)
        out = source.known_supported_records_formats()
        self.assertEqual(out, [mock_records_format])

    def test_can_move_to_format_yes(self):
        mock_records_format = Mock(name='records_format')
        mock_records_schema = Mock(name='records_schema')
        mock_records_schema = Mock(name='records_schema')
        mock_target_names_to_input_fileobjs = Mock(name='target_names_to_input_fileobjs')
        source = FileobjsSource(target_names_to_input_fileobjs=mock_target_names_to_input_fileobjs,
                                records_schema=mock_records_schema,
                                records_format=mock_records_format)
        out = source.can_move_to_format(mock_records_format)
        self.assertTrue(out)

    def test_can_move_to_schema_yes(self):
        mock_records_format = Mock(name='records_format')
        mock_records_schema = Mock(name='records_schema')
        mock_records_schema = Mock(name='records_schema')
        mock_target_names_to_input_fileobjs = Mock(name='target_names_to_input_fileobjs')
        source = FileobjsSource(target_names_to_input_fileobjs=mock_target_names_to_input_fileobjs,
                                records_schema=mock_records_schema,
                                records_format=mock_records_format)
        out = source.can_move_to_scheme(Mock())
        self.assertTrue(out)

    def test_str(self):
        mock_records_format = 'mumble'
        mock_records_schema = Mock(name='records_schema')
        mock_target_names_to_input_fileobjs = Mock(name='target_names_to_input_fileobjs')
        source = FileobjsSource(target_names_to_input_fileobjs=mock_target_names_to_input_fileobjs,
                                records_schema=mock_records_schema,
                                records_format=mock_records_format)
        self.assertEqual(str(source), 'FileobjsSource(mumble)')

    @patch('records_mover.records.pandas.pandas_read_csv_options')
    @patch('records_mover.records.sources.fileobjs.io')
    @patch('pandas.read_csv')
    def test_to_dataframes_source(self, mock_read_csv, mock_io, mock_pandas_read_csv_options):
        def read_csv_options(records_format,
                             records_schema,
                             unhandled_hints,
                             processing_instructions):
            unhandled_hints.clear()
            return {}

        mock_pandas_read_csv_options.side_effect = read_csv_options
        mock_records_format = Mock(name='records_format', spec=DelimitedRecordsFormat)
        mock_records_format.hints = {
            'compression': None,
            'encoding': 'UTF8',
        }
        mock_records_schema = Mock(name='records_schema')
        mock_field = Mock(name='field')
        mock_records_schema.fields = [mock_field]
        mock_fileobj = Mock(name='fileobj')
        mock_target_names_to_input_fileobjs = {
            'name': mock_fileobj
        }
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_reader = mock_read_csv.return_value
        mock_dfs = mock_reader
        source = FileobjsSource(target_names_to_input_fileobjs=mock_target_names_to_input_fileobjs,
                                records_schema=mock_records_schema,
                                records_format=mock_records_format)
        with source.to_dataframes_source(mock_processing_instructions) as df_source:
            self.assertEqual(df_source.dfs, mock_dfs)

    @patch('records_mover.records.sources.fileobjs.MoveResult')
    def test_move_to_records_directory(self,
                                       mock_MoveResult):
        mock_records_format = Mock(name='records_format')
        mock_records_schema = Mock(name='records_schema')
        mock_target_names_to_input_fileobjs = Mock(name='target_names_to_input_fileobjs')
        mock_processing_instructions = Mock(name='processing_instructions')

        source = FileobjsSource(target_names_to_input_fileobjs=mock_target_names_to_input_fileobjs,
                                records_schema=mock_records_schema,
                                records_format=mock_records_format)

        mock_records_directory = Mock(name='record_directory')
        mock_url = 'vmb://dir/file.mumble'
        mock_url_detail = Mock(name='url_detail')
        mock_url_details = {
            mock_url: mock_url_detail
        }

        mock_records_directory.save_fileobjs.return_value = mock_url_details

        out = source.move_to_records_directory(mock_records_directory,
                                               mock_records_format,
                                               mock_processing_instructions)
        mock_records_directory.save_fileobjs.assert_called_with(mock_target_names_to_input_fileobjs,
                                                                records_format=mock_records_format,
                                                                records_schema=mock_records_schema)
        mock_MoveResult.assert_called_with(move_count=None,
                                           output_urls={'file.mumble': 'vmb://dir/file.mumble'})
        self.assertEqual(out, mock_MoveResult.return_value)
