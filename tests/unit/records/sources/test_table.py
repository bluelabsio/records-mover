from records_mover.records.sources.table import TableRecordsSource
from mock import Mock, patch, ANY
import unittest
from records_mover.records.targets.base import MightSupportMoveFromTempLocAfterFillingIt


class TestTableRecordsSource(unittest.TestCase):
    def setUp(self):
        self.mock_schema_name = Mock(name='schema_name')
        self.mock_table_name = Mock(name='table_name')
        self.mock_driver = Mock(name='driver')
        self.mock_loader = self.mock_driver.loader.return_value
        self.mock_unloader = self.mock_driver.unloader.return_value
        self.mock_db_driver = Mock(name='db_driver')
        self.mock_url_resolver = Mock(name='url_resolver')
        self.table_records_source =\
            TableRecordsSource(schema_name=self.mock_schema_name,
                               table_name=self.mock_table_name,
                               driver=self.mock_driver,
                               url_resolver=self.mock_url_resolver)

    @patch('records_mover.records.sources.dataframes.DataframesRecordsSource')
    @patch('records_mover.records.sources.table.RecordsSchema')
    @patch('records_mover.records.sources.table.quote_schema_and_table')
    @patch('pandas.read_sql')
    def test_to_dataframes_source(self,
                                  mock_read_sql,
                                  mock_quote_schema_and_table,
                                  mock_RecordsSchema,
                                  mock_DataframesRecordsSource):
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_records_schema = mock_RecordsSchema.from_db_table.return_value
        mock_db = self.mock_driver.db
        mock_column = Mock(name='column')
        mock_columns = [mock_column]
        mock_db.dialect.get_columns.return_value = mock_columns
        mock_quoted_table = mock_quote_schema_and_table.return_value
        with self.table_records_source.to_dataframes_source(mock_processing_instructions) as\
                df_source:
            self.assertEqual(df_source, mock_DataframesRecordsSource.return_value)
            mock_db.dialect.get_columns.assert_called_with(mock_db,
                                                           self.mock_table_name,
                                                           schema=self.mock_schema_name)
            mock_RecordsSchema.from_db_table.assert_called_with(self.mock_schema_name,
                                                                self.mock_table_name,
                                                                driver=self.mock_driver)
            mock_read_sql.assert_called_with(f"SELECT * FROM {mock_quoted_table}",
                                             chunksize=2000000,
                                             con=mock_db)
            mock_DataframesRecordsSource.\
                assert_called_with(dfs=ANY,
                                   processing_instructions=mock_processing_instructions,
                                   records_schema=mock_records_schema)

    @patch('records_mover.records.sources.table.RecordsUnloadPlan')
    @patch('records_mover.records.sources.table.MoveResult')
    @patch('records_mover.records.sources.table.RecordsSchema')
    def test_move_to_records_directory(self,
                                       mock_RecordsSchema,
                                       mock_MoveResult,
                                       mock_RecordsUnloadPlan):
        mock_records_directory = Mock(name='records_directory')
        mock_records_format = Mock(name='records_format')
        mock_processing_instructions = Mock(name='processing_instructions')
        out = self.table_records_source.move_to_records_directory(mock_records_directory,
                                                                  mock_records_format,
                                                                  mock_processing_instructions)
        mock_RecordsUnloadPlan.\
            assert_called_with(records_format=mock_records_format,
                               processing_instructions=mock_processing_instructions)
        mock_unload_plan = mock_RecordsUnloadPlan.return_value
        self.mock_unloader.unload.assert_called_with(schema=self.mock_schema_name,
                                                     table=self.mock_table_name,
                                                     unload_plan=mock_unload_plan,
                                                     directory=mock_records_directory)
        mock_export_count = self.mock_unloader.unload.return_value
        mock_records_directory.save_schema.\
            assert_called_with(self.mock_driver.tweak_records_schema_after_unload.return_value)
        mock_records_directory.save_format.assert_called_with(mock_unload_plan.records_format)
        mock_records_directory.finalize_manifest.assert_called_with()
        mock_MoveResult.assert_called_with(move_count=mock_export_count,
                                           output_urls=None)
        mock_result = mock_MoveResult.return_value
        self.assertEqual(out, mock_result)

    @patch('records_mover.records.sources.table.RecordsUnloadPlan')
    @patch('records_mover.records.sources.table.MoveResult')
    @patch('records_mover.records.sources.table.RecordsSchema')
    def test_has_compatible_format_1(self,
                                     mock_RecordsSchema,
                                     mock_MoveResult,
                                     mock_RecordsUnloadPlan):
        mock_records_target = Mock(name='records_target',
                                   spec=MightSupportMoveFromTempLocAfterFillingIt)
        mock_source_format_1 = Mock(name='source_format_1')
        mock_target_format_1 = Mock(name='target_format_1')
        mock_common_format = Mock(name='common_format')
        mock_source_formats = [mock_source_format_1, mock_common_format]
        mock_target_formats = [mock_target_format_1, mock_common_format]
        mock_records_target.known_supported_records_formats.return_value = mock_target_formats
        self.mock_unloader.known_supported_records_formats_for_unload.return_value =\
            mock_source_formats
        self.mock_unloader.known_supported_records_formats_for_unload.return_value =\
            mock_source_formats

        def target_can_move_from_format(source_candidate):
            return (source_candidate == mock_common_format or
                    source_candidate == mock_target_format_1)

        def source_can_move_to_format(target_candidate):
            return (target_candidate == mock_common_format or
                    target_candidate == mock_source_format_1)

        self.mock_loader.can_move_to_format = source_can_move_to_format

        mock_records_target.can_move_from_format = target_can_move_from_format
        out = self.table_records_source.has_compatible_format(mock_records_target)
        self.mock_unloader.known_supported_records_formats_for_unload.assert_called_with()
        mock_records_target.known_supported_records_formats.assert_called_with()
        self.assertEqual(True, out)

    @patch('records_mover.records.sources.table.RecordsUnloadPlan')
    @patch('records_mover.records.sources.table.MoveResult')
    @patch('records_mover.records.sources.table.RecordsSchema')
    def test_has_compatible_format_2(self,
                                     mock_RecordsSchema,
                                     mock_MoveResult,
                                     mock_RecordsUnloadPlan):
        mock_records_target = Mock(name='records_target',
                                   spec=MightSupportMoveFromTempLocAfterFillingIt)
        mock_source_format_1 = Mock(name='source_format_1')
        mock_target_format_1 = Mock(name='target_format_1')
        mock_common_format = Mock(name='common_format')
        mock_source_formats = [mock_source_format_1, mock_common_format]
        mock_target_formats = [mock_target_format_1, mock_common_format]
        mock_records_target.known_supported_records_formats.return_value = mock_target_formats
        self.mock_unloader.known_supported_records_formats_for_unload.return_value =\
            mock_source_formats
        self.mock_unloader.known_supported_records_formats_for_unload.return_value =\
            mock_source_formats

        def target_can_move_from_format(source_candidate):
            return source_candidate in [mock_common_format,
                                        mock_target_format_1,
                                        mock_source_format_1]

        def source_can_move_to_format(target_candidate):
            return target_candidate in [mock_common_format, mock_source_format_1]

        self.mock_driver.can_move_to_format = source_can_move_to_format

        mock_records_target.can_move_from_format = target_can_move_from_format
        out = self.table_records_source.has_compatible_format(mock_records_target)
        self.mock_unloader.known_supported_records_formats_for_unload.assert_called_with()
        mock_records_target.known_supported_records_formats.assert_called_with()
        self.assertEqual(True, out)

    def test_known_supported_records_formats_no_unloader(self):
        self.mock_driver.unloader.return_value = None
        self.assertEqual([], self.table_records_source.known_supported_records_formats())

    def test_str(self):
        self.mock_driver.db_engine.name = 'george'
        self.assertEqual(str(self.table_records_source), 'TableRecordsSource(george)')

    def test_with_cast_dataframe_types(self):
        mock_df_1 = Mock(name='df_1')
        mock_df_2 = Mock(name='df_2')
        mock_dfs = [mock_df_1, mock_df_2]
        mock_records_schema = Mock(name='records_schema')
        out = self.table_records_source.with_cast_dataframe_types(mock_records_schema, mock_dfs)
        self.assertEqual(list(out), [
            mock_records_schema.cast_dataframe_types.return_value,
            mock_records_schema.cast_dataframe_types.return_value,
        ])

    def test_can_move_to_scheme(self):
        out = self.table_records_source.can_move_to_scheme(Mock())
        self.assertEqual(out,
                         self.mock_unloader.can_unload_to_scheme.return_value)
