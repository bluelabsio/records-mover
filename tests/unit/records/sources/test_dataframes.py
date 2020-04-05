from records_mover.records.sources.dataframes import DataframesRecordsSource
from records_mover.records.records_format import DelimitedRecordsFormat, ParquetRecordsFormat
from mock import Mock, patch, mock_open
import unittest


class TestDataframesRecordsSource(unittest.TestCase):
    @patch('records_mover.records.sources.dataframes.format_df_for_csv_output')
    @patch('records_mover.records.sources.dataframes.purge_unnamed_unused_columns')
    @patch('records_mover.records.sources.dataframes.RecordsSchema')
    @patch('records_mover.records.sources.dataframes.FileobjsSource')
    @patch('records_mover.records.sources.dataframes.complain_on_unhandled_hints')
    @patch('records_mover.records.sources.dataframes.pandas_to_csv_options')
    @patch('records_mover.records.sources.dataframes.NamedTemporaryFile')
    @patch("builtins.open", new_callable=mock_open)
    def test_to_delimited_fileobjs_source(self,
                                          mock_builtin_open,
                                          mock_NamedTemporaryFile,
                                          mock_pandas_to_csv_options,
                                          mock_complain_on_unhandled_hints,
                                          mock_FileobjsSource,
                                          mock_RecordsSchema,
                                          mock_purge_unnamed_unused_columns,
                                          mock_format_df_for_csv_output):
        mock_df_1 = Mock(name='df_1')
        mock_df_2 = Mock(name='df_2')
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_processing_instructions.max_inference_rows = 10
        mock_include_index = Mock(name='include_index')
        dataframe_records_source =\
            DataframesRecordsSource(dfs=[mock_df_1, mock_df_2],
                                    processing_instructions=mock_processing_instructions,
                                    include_index=mock_include_index)

        mock_target_records_format = Mock(name='target_records_format', spec=DelimitedRecordsFormat)
        mock_target_records_format.hints = Mock(name='hints')
        mock_target_records_format.hints.keys.return_value = []
        mock_unhandled_hints = set(['compression'])
        mock_options = mock_pandas_to_csv_options.return_value
        mock_output_file = mock_NamedTemporaryFile.return_value.__enter__.return_value
        mock_output_filename = mock_output_file.name
        mock_data_fileobj_1 = mock_builtin_open.return_value.__enter__.return_value
        mock_data_fileobj_1.closed = False
        mock_data_fileobj_2 = mock_builtin_open.return_value.__enter__.return_value
        mock_data_fileobj_2.closed = False
        mock_target_records_format.hints = {'compression': None}

        def generate_filename(prefix):
            return f"{prefix}.csv"

        mock_target_records_format.generate_filename = generate_filename
        mock_target_records_schema = mock_RecordsSchema.from_dataframe.return_value
        mock_purge_unnamed_unused_columns.side_effect = lambda a: a
        with dataframe_records_source.\
            to_fileobjs_source(records_format_if_possible=mock_target_records_format,
                               processing_instructions=mock_processing_instructions)\
                as fileobjs:
            mock_pandas_to_csv_options.assert_called_with(mock_target_records_format,
                                                          mock_unhandled_hints,
                                                          mock_processing_instructions)
            mock_complain_on_unhandled_hints.\
                assert_called_with(mock_processing_instructions.fail_if_dont_understand,
                                   mock_unhandled_hints,
                                   mock_target_records_format.hints)
            mock_format_df_for_csv_output.assert_any_call(mock_df_1,
                                                          mock_target_records_schema,
                                                          mock_target_records_format)
            mock_format_df_for_csv_output.assert_any_call(mock_df_2,
                                                          mock_target_records_schema,
                                                          mock_target_records_format)
            mock_formatted_df = mock_format_df_for_csv_output.return_value
            mock_formatted_df.to_csv.assert_called_with(path_or_buf=mock_output_filename,
                                                        index=mock_include_index,
                                                        **mock_options)
            mock_FileobjsSource.\
                assert_called_with(target_names_to_input_fileobjs={
                    "data001.csv": mock_data_fileobj_1,
                    "data002.csv": mock_data_fileobj_2,
                },
                                   records_schema=mock_target_records_schema,
                                   records_format=mock_target_records_format)
            self.assertEqual(fileobjs, mock_FileobjsSource.return_value)
            mock_data_fileobj_1.close.assert_not_called()
            mock_data_fileobj_2.close.assert_not_called()

        mock_data_fileobj_1.close.assert_called()
        mock_data_fileobj_2.close.assert_called()

    @patch('records_mover.records.sources.dataframes.purge_unnamed_unused_columns')
    @patch('records_mover.records.sources.dataframes.RecordsSchema')
    @patch('records_mover.records.sources.dataframes.FileobjsSource')
    @patch('records_mover.records.sources.dataframes.complain_on_unhandled_hints')
    @patch('records_mover.records.sources.dataframes.pandas_to_csv_options')
    @patch('records_mover.records.sources.dataframes.NamedTemporaryFile')
    @patch("builtins.open", new_callable=mock_open)
    def test_to_parquet_fileobjs_source(self,
                                        mock_builtin_open,
                                        mock_NamedTemporaryFile,
                                        mock_pandas_to_csv_options,
                                        mock_complain_on_unhandled_hints,
                                        mock_FileobjsSource,
                                        mock_RecordsSchema,
                                        mock_purge_unnamed_unused_columns):
        mock_df_1 = Mock(name='df_1')
        mock_df_2 = Mock(name='df_2')
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_processing_instructions.max_inference_rows = 10
        mock_include_index = Mock(name='include_index')
        dataframe_records_source =\
            DataframesRecordsSource(dfs=[mock_df_1, mock_df_2],
                                    processing_instructions=mock_processing_instructions,
                                    include_index=mock_include_index)

        mock_target_records_format = Mock(name='target_records_format', spec=ParquetRecordsFormat)
        mock_target_records_format.hints = {'compression': None}

        def generate_filename(prefix):
            return f"{prefix}.parquet"

        mock_target_records_format.generate_filename = generate_filename

        mock_target_records_schema = mock_RecordsSchema.from_dataframe.return_value
        mock_output_file = mock_NamedTemporaryFile.return_value.__enter__.return_value
        mock_output_filename = mock_output_file.name

        mock_data_fileobj_1 = mock_builtin_open.return_value.__enter__.return_value
        mock_data_fileobj_1.closed = False
        mock_data_fileobj_2 = mock_builtin_open.return_value.__enter__.return_value
        mock_data_fileobj_2.closed = False
        mock_purge_unnamed_unused_columns.side_effect = lambda a: a

        with dataframe_records_source.\
            to_fileobjs_source(records_format_if_possible=mock_target_records_format,
                               processing_instructions=mock_processing_instructions)\
                as fileobjs:
            mock_builtin_open.assert_called_with(mock_output_filename, 'rb')

            mock_df_1.to_parquet.assert_called_with(fname=mock_output_filename,
                                                    engine='pyarrow',
                                                    coerce_timestamps=None,
                                                    index=mock_include_index)
            mock_df_2.to_parquet.assert_called_with(fname=mock_output_filename,
                                                    engine='pyarrow',
                                                    coerce_timestamps=None,
                                                    index=mock_include_index)
            mock_FileobjsSource.\
                assert_called_with(target_names_to_input_fileobjs={
                    "data001.parquet": mock_data_fileobj_1,
                    "data002.parquet": mock_data_fileobj_2,
                },
                                   records_schema=mock_target_records_schema,
                                   records_format=mock_target_records_format)
            self.assertEqual(fileobjs, mock_FileobjsSource.return_value)
            mock_data_fileobj_1.close.assert_not_called()
            mock_data_fileobj_2.close.assert_not_called()

        mock_data_fileobj_1.close.assert_called()
        mock_data_fileobj_2.close.assert_called()
