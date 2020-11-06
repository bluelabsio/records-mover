import unittest
from records_mover.url.base import BaseFileUrl
from records_mover.records.targets.data_url import DataUrlTarget
from records_mover.records.records_directory import RecordsDirectory
from records_mover.records.base_records_format import BaseRecordsFormat
from records_mover.records.processing_instructions import ProcessingInstructions
from mock import patch, Mock, MagicMock


class TestDataUrlTarget(unittest.TestCase):
    @patch('records_mover.records.targets.data_url.isinstance')
    @patch('records_mover.records.targets.data_url.DelimitedRecordsFormat')
    @patch('records_mover.records.targets.data_url.FileobjTarget')
    def test_move_from_dataframe_csv_extension(self,
                                               mock_FileobjTarget,
                                               mock_DelimitedRecordsFormat,
                                               mock_isinstance):
        mock_output_url = 'whatever://foo/foo.csv'
        mock_output_loc = MagicMock(name='output_loc', spec=BaseFileUrl)
        mock_output_loc.url = mock_output_url
        mock_default_records_format = mock_DelimitedRecordsFormat.return_value
        mock_records_format = mock_default_records_format.alter_hints.return_value
        data_url_target = DataUrlTarget(output_loc=mock_output_loc,
                                        records_format=None)
        mock_default_records_format.alter_hints.assert_called_with({'compression': None})

        mock_fileobj = mock_output_loc.open.return_value.__enter__.return_value

        mock_df = Mock(name='df')
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_dfs_source = Mock(name='dfs_source')
        mock_dfs_source.dfs = [mock_df]
        out = data_url_target.move_from_dataframes_source(mock_dfs_source,
                                                          mock_processing_instructions)
        mock_FileobjTarget.assert_called_with(fileobj=mock_fileobj,
                                              records_format=mock_records_format)
        mock_output_loc.open.assert_called_with(mode='wb')
        mock_fileobj_target = mock_FileobjTarget.return_value
        mock_fileobj_target.move_from_dataframes_source.\
            assert_called_with(dfs_source=mock_dfs_source,
                               processing_instructions=mock_processing_instructions)
        self.assertEqual(mock_fileobj_target.move_from_dataframes_source.return_value, out)

    @patch('records_mover.records.targets.data_url.isinstance')
    @patch('records_mover.records.targets.data_url.DelimitedRecordsFormat')
    @patch('records_mover.records.targets.data_url.FileobjTarget')
    def test_move_from_dataframe_gz_extension(self,
                                              mock_FileobjTarget,
                                              mock_DelimitedRecordsFormat,
                                              mock_isinstance):
        mock_output_url = 'whatever://foo/foo.csv.gz'
        mock_output_loc = MagicMock(name='output_loc', spec=BaseFileUrl)
        mock_output_loc.url = mock_output_url
        mock_default_records_format = mock_DelimitedRecordsFormat.return_value
        mock_records_format = mock_default_records_format.alter_hints.return_value
        data_url_target = DataUrlTarget(output_loc=mock_output_loc,
                                        records_format=None)

        mock_default_records_format.alter_hints.assert_called_with({'compression': 'GZIP'})

        mock_fileobj = mock_output_loc.open.return_value.__enter__.return_value

        mock_df = Mock(name='df')
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_dfs_source = Mock(name='dfs_source')
        mock_dfs_source.dfs = [mock_df]
        out = data_url_target.\
            move_from_dataframes_source(dfs_source=mock_dfs_source,
                                        processing_instructions=mock_processing_instructions)
        mock_FileobjTarget.assert_called_with(fileobj=mock_fileobj,
                                              records_format=mock_records_format)
        mock_output_loc.open.assert_called_with(mode='wb')
        mock_fileobj_target = mock_FileobjTarget.return_value
        mock_fileobj_target.move_from_dataframes_source.\
            assert_called_with(dfs_source=mock_dfs_source,
                               processing_instructions=mock_processing_instructions)
        self.assertEqual(mock_fileobj_target.move_from_dataframes_source.return_value, out)

    def test_can_move_directly_from_scheme(self):
        mock_output_url = 'whatever://foo/foo.csv.gz'
        mock_output_loc = MagicMock(name='output_loc', spec=BaseFileUrl)
        mock_output_loc.scheme = 'whatever'
        mock_output_loc.url = mock_output_url
        data_url_target = DataUrlTarget(output_loc=mock_output_loc,
                                        records_format=None)
        # Neither can load direct right now, as no schemes have a
        # 'remote copy' option that doesn't involve streaming things
        # over the network to RecordsMover and back.
        self.assertFalse(data_url_target.can_move_directly_from_scheme(mock_output_loc.scheme))
        self.assertFalse(data_url_target.can_move_directly_from_scheme('something_else'))

    @patch('records_mover.records.targets.data_url.MoveResult')
    def test_move_from_records_directory(self, mock_MoveResult):
        mock_output_url = 'whatever://foo/foo.csv.gz'
        mock_output_loc = MagicMock(name='output_loc', spec=BaseFileUrl)
        mock_output_loc.url = mock_output_url
        data_url_target = DataUrlTarget(output_loc=mock_output_loc,
                                        records_format=None)
        mock_directory = Mock(name='directory', spec=RecordsDirectory)
        mock_processing_instructions = Mock(name='processing_instructions',
                                            spec=ProcessingInstructions)
        mock_processing_instructions.fail_if_dont_understand = Mock(name='fail_if_dont_understand',
                                                                    spec=bool)
        mock_override_records_format = Mock(name='override_records_format', spec=BaseRecordsFormat)
        mock_records_format = mock_directory.load_format.return_value
        out = data_url_target.move_from_records_directory(mock_directory,
                                                          mock_processing_instructions,
                                                          mock_override_records_format)
        mock_directory.load_format.\
            assert_called_with(mock_processing_instructions.fail_if_dont_understand)
        mock_directory.save_to_url.assert_called_with(mock_output_loc)
        mock_MoveResult.assert_called_with(move_count=None,
                                           output_urls={
                                               mock_records_format.generate_filename.return_value:
                                               mock_output_loc.url
                                           })
        mock_records_format.generate_filename.assert_called_with('data')
        self.assertEqual(out, mock_MoveResult.return_value)
