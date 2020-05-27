import unittest
from records_mover.records.targets.fileobj import FileobjTarget
from records_mover.records.results import MoveResult
from records_mover.records.records_format import DelimitedRecordsFormat
from mock import patch, Mock, ANY


class TestFileobjTarget(unittest.TestCase):
    @patch('records_mover.records.pandas.prep_df_for_csv_output')
    @patch('records_mover.records.targets.fileobj.io')
    @patch('records_mover.records.targets.fileobj.complain_on_unhandled_hints')
    def test_move_from_dataframe_uncompressed_no_header_row(self,
                                                            mock_complain_on_unhandled_hints,
                                                            mock_io,
                                                            mock_prep_df_for_csv_output):
        mock_fileobj = Mock(name='fileobj')
        mock_records_format = DelimitedRecordsFormat(hints={
            'encoding': 'UTF8',
            'compression': None,
            'header-row': False,
            'quoting': 'all'
        })
        fileobj_target = FileobjTarget(fileobj=mock_fileobj,
                                       records_format=mock_records_format)

        mock_df_1 = Mock(name='df_1')
        mock_df_1.index = ['a']
        mock_df_2 = Mock(name='df_2')
        mock_df_2.index = ['a']
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_dfs_source = Mock(name='dfs_source')
        mock_dfs_source.dfs = [mock_df_1, mock_df_2]
        mock_prep_df_for_csv_output.side_effect = [mock_df_1, mock_df_2]
        out = fileobj_target.move_from_dataframes_source(mock_dfs_source,
                                                         mock_processing_instructions)
        mock_text_fileobj = mock_io.TextIOWrapper.return_value
        mock_df_1.to_csv.assert_called_with(index=mock_dfs_source.include_index,
                                            path_or_buf=mock_text_fileobj,
                                            mode="a",
                                            date_format='%Y-%m-%d %H:%M:%S.%f%z',
                                            doublequote=False,
                                            encoding='UTF8',
                                            escapechar='\\',
                                            header=False,
                                            line_terminator='\n',
                                            quotechar='"',
                                            quoting=1,
                                            sep=',')
        mock_df_2.to_csv.assert_called_with(index=mock_dfs_source.include_index,
                                            path_or_buf=mock_text_fileobj,
                                            mode="a",
                                            date_format='%Y-%m-%d %H:%M:%S.%f%z',
                                            doublequote=False,
                                            encoding='UTF8',
                                            escapechar='\\',
                                            header=False,
                                            line_terminator='\n',
                                            quotechar='"',
                                            quoting=1,
                                            sep=',')
        self.assertEqual(out, MoveResult(move_count=2, output_urls=None))

    @patch('records_mover.records.pandas.prep_df_for_csv_output')
    @patch('records_mover.records.targets.fileobj.io')
    @patch('records_mover.records.targets.fileobj.complain_on_unhandled_hints')
    def test_move_from_dataframe_uncompressed_with_header_row(self,
                                                              mock_complain_on_unhandled_hints,
                                                              mock_io,
                                                              mock_prep_df_for_csv_output):
        mock_fileobj = Mock(name='fileobj')
        mock_records_format = DelimitedRecordsFormat(hints={
            'encoding': 'UTF8',
            'compression': None,
            'header-row': True,
            'quoting': 'all'
        })
        fileobj_target = FileobjTarget(fileobj=mock_fileobj,
                                       records_format=mock_records_format)

        mock_df_1 = Mock(name='df_1')
        mock_df_1.index = ['a']
        mock_df_2 = Mock(name='df_2')
        mock_df_2.index = ['a']
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_dfs_source = Mock(name='dfs_source')
        mock_dfs_source.dfs = [mock_df_1, mock_df_2]
        mock_prep_df_for_csv_output.side_effect = [mock_df_1, mock_df_2]
        out = fileobj_target.move_from_dataframes_source(mock_dfs_source,
                                                         mock_processing_instructions)
        mock_text_fileobj = mock_io.TextIOWrapper.return_value
        mock_df_1.to_csv.assert_called_with(index=mock_dfs_source.include_index,
                                            path_or_buf=mock_text_fileobj,
                                            mode="a",
                                            date_format='%Y-%m-%d %H:%M:%S.%f%z',
                                            doublequote=False,
                                            encoding='UTF8',
                                            escapechar='\\',
                                            header=True,
                                            line_terminator='\n',
                                            quotechar='"',
                                            quoting=1,
                                            sep=',')
        mock_df_2.to_csv.assert_called_with(index=mock_dfs_source.include_index,
                                            path_or_buf=mock_text_fileobj,
                                            mode="a",
                                            date_format='%Y-%m-%d %H:%M:%S.%f%z',
                                            doublequote=False,
                                            encoding='UTF8',
                                            escapechar='\\',
                                            header=False,
                                            line_terminator='\n',
                                            quotechar='"',
                                            quoting=1,
                                            sep=',')
        self.assertEqual(out, MoveResult(move_count=2, output_urls=None))

    @patch('records_mover.records.pandas.prep_df_for_csv_output')
    @patch('records_mover.records.targets.fileobj.io')
    @patch('records_mover.records.targets.fileobj.complain_on_unhandled_hints')
    def test_move_from_dataframe_compressed_no_header_row(self,
                                                          mock_complain_on_unhandled_hints,
                                                          mock_io,
                                                          mock_prep_df_for_csv_output):
        mock_fileobj = Mock(name='fileobj')
        mock_records_format = DelimitedRecordsFormat(hints={
            'encoding': 'UTF8',
            'compression': 'GZIP',
            'header-row': False,
            'quoting': 'all'
        })
        fileobj_target = FileobjTarget(fileobj=mock_fileobj,
                                       records_format=mock_records_format)

        mock_df_1 = Mock(name='df_1')
        mock_df_1.index = ['a']
        mock_df_2 = Mock(name='df_2')
        mock_df_2.index = ['a']
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_dfs_source = Mock(name='dfs_source')
        mock_dfs_source.dfs = [mock_df_1, mock_df_2]
        mock_prep_df_for_csv_output.side_effect = [mock_df_1, mock_df_2]
        out = fileobj_target.move_from_dataframes_source(mock_dfs_source,
                                                         mock_processing_instructions)
        mock_df_1.to_csv.assert_called_with(path_or_buf=ANY,
                                            index=mock_dfs_source.include_index,
                                            mode="a",
                                            compression='gzip',
                                            date_format='%Y-%m-%d %H:%M:%S.%f%z',
                                            doublequote=False,
                                            encoding='UTF8',
                                            escapechar='\\',
                                            header=False,
                                            line_terminator='\n',
                                            quotechar='"',
                                            quoting=1,
                                            sep=',')
        mock_df_2.to_csv.assert_called_with(path_or_buf=ANY,
                                            index=mock_dfs_source.include_index,
                                            mode="a",
                                            compression='gzip',
                                            date_format='%Y-%m-%d %H:%M:%S.%f%z',
                                            doublequote=False,
                                            encoding='UTF8',
                                            escapechar='\\',
                                            header=False,
                                            line_terminator='\n',
                                            quotechar='"',
                                            quoting=1,
                                            sep=',')
        self.assertEqual(out, MoveResult(move_count=2, output_urls=None))

    @patch('records_mover.records.pandas.prep_df_for_csv_output')
    @patch('records_mover.records.targets.fileobj.io')
    @patch('records_mover.records.targets.fileobj.complain_on_unhandled_hints')
    def test_move_from_dataframe_compressed_with_header_row(self,
                                                            mock_complain_on_unhandled_hints,
                                                            mock_io,
                                                            mock_prep_df_for_csv_output):
        mock_fileobj = Mock(name='fileobj')
        mock_records_format = DelimitedRecordsFormat(hints={
            'encoding': 'UTF8',
            'compression': 'GZIP',
            'header-row': True,
            'quoting': 'all'
        })
        fileobj_target = FileobjTarget(fileobj=mock_fileobj,
                                       records_format=mock_records_format)

        mock_df_1 = Mock(name='df_1')
        mock_df_1.index = ['a']
        mock_df_2 = Mock(name='df_2')
        mock_df_2.index = ['a']
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_dfs_source = Mock(name='dfs_source')
        mock_dfs_source.dfs = [mock_df_1, mock_df_2]
        mock_prep_df_for_csv_output.side_effect = [mock_df_1, mock_df_2]
        out = fileobj_target.move_from_dataframes_source(mock_dfs_source,
                                                         mock_processing_instructions)
        mock_df_1.to_csv.assert_called_with(path_or_buf=ANY,
                                            index=mock_dfs_source.include_index,
                                            mode="a",
                                            compression='gzip',
                                            date_format='%Y-%m-%d %H:%M:%S.%f%z',
                                            doublequote=False,
                                            encoding='UTF8',
                                            escapechar='\\',
                                            header=True,
                                            line_terminator='\n',
                                            quotechar='"',
                                            quoting=1,
                                            sep=',')
        mock_df_2.to_csv.assert_called_with(path_or_buf=ANY,
                                            index=mock_dfs_source.include_index,
                                            mode="a",
                                            compression='gzip',
                                            date_format='%Y-%m-%d %H:%M:%S.%f%z',
                                            doublequote=False,
                                            encoding='UTF8',
                                            escapechar='\\',
                                            header=False,
                                            line_terminator='\n',
                                            quotechar='"',
                                            quoting=1,
                                            sep=',')
        self.assertEqual(out, MoveResult(move_count=2, output_urls=None))
