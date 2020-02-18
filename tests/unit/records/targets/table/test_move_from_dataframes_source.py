import unittest
from mock import MagicMock, Mock, patch
from records_mover.records.prep import TablePrep
from records_mover.records.targets.table.move_from_dataframes_source import (
    DoMoveFromDataframesSource
)


class TestDoMoveFromDataframesSource(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_prep = Mock(name='prep', spec=TablePrep)
        self.mock_tbl = MagicMock(name='tbl')
        self.mock_processing_instructions = Mock(name='processing_instructions',
                                                 spec='ProcessingInstructions')
        self.mock_dfs_source = MagicMock(name='dfs_source')
        self.mock_table_target = Mock(name='table_target')
        self.mock_table_target = Mock(name='table_target')
        self.algo =\
            DoMoveFromDataframesSource(prep=self.mock_prep,
                                       table_target=self.mock_table_target,
                                       target_table_details=self.mock_tbl,
                                       dfs_source=self.mock_dfs_source,
                                       processing_instructions=self.mock_processing_instructions)

    def test_move_via_fileobjs_load(self):
        mock_records_format = Mock(name='records_format')
        self.mock_table_target.known_supported_records_formats.return_value = [mock_records_format]
        self.mock_table_target.can_move_from_fileobjs_source.return_value = True
        mock_fileobjs_source =\
            self.mock_dfs_source.to_fileobjs_source.return_value.__enter__.return_value
        expected_ret = self.mock_table_target.move_from_fileobjs_source.return_value

        out = self.algo.move()
        self.mock_dfs_source.to_fileobjs_source.\
            assert_called_with(self.mock_processing_instructions, mock_records_format)
        self.mock_table_target.move_from_fileobjs_source.\
            assert_called_with(mock_fileobjs_source,
                               self.mock_processing_instructions)
        self.assertEqual(out, expected_ret)

    def test_move_via_records_directory_load(self):
        mock_records_format = Mock(name='records_format')
        self.mock_table_target.known_supported_records_formats.return_value = [mock_records_format]
        self.mock_table_target.can_move_from_fileobjs_source.return_value = False
        mock_fileobjs_source =\
            self.mock_dfs_source.to_fileobjs_source.return_value.__enter__.return_value
        expected_ret = self.mock_table_target.move_from_temp_loc_after_filling_it.return_value

        out = self.algo.move()
        self.mock_dfs_source.to_fileobjs_source.\
            assert_called_with(self.mock_processing_instructions, mock_records_format)
        self.mock_table_target.move_from_temp_loc_after_filling_it.\
            assert_called_with(mock_fileobjs_source,
                               self.mock_processing_instructions)
        self.assertEqual(out, expected_ret)

    @patch('records_mover.records.targets.table.move_from_dataframes_source.prep_and_load')
    def test_move_via_insert(self, mock_prep_and_load):
        self.mock_table_target.known_supported_records_formats.return_value = []
        mock_driver = self.mock_tbl.db_driver.return_value
        mock_records_schema = self.mock_dfs_source.initial_records_schema.return_value
        mock_schema_sql = mock_records_schema.to_schema_sql.return_value
        out = self.algo.move()
        self.mock_tbl.db_driver.assert_called_with(self.mock_tbl.db_engine)
        self.mock_dfs_source.initial_records_schema.\
            assert_called_with(self.mock_processing_instructions)
        mock_records_schema.to_schema_sql.assert_called_with(mock_driver,
                                                             self.mock_tbl.schema_name,
                                                             self.mock_tbl.table_name)
        mock_prep_and_load.assert_called_with(self.mock_tbl, self.mock_prep, mock_schema_sql,
                                              self.algo.load)
        self.assertEqual(out, mock_prep_and_load.return_value)

    @patch('records_mover.records.targets.table.move_from_dataframes_source.' +
           'purge_unnamed_unused_columns')
    def test_load(self, mock_purge_unnamed_unused_columns):
        mock_driver = Mock(name='driver')
        mock_df = Mock(name='df')
        mock_records_schema = self.mock_dfs_source.initial_records_schema.return_value
        self.mock_dfs_source.dfs = [mock_df]
        mock_db = self.mock_tbl.db_engine.begin.return_value.__enter__.return_value
        mock_df_1 = mock_purge_unnamed_unused_columns.return_value
        mock_df_2 = mock_records_schema.assign_dataframe_names.return_value
        mock_df_2.index = [1, 2, 3]
        out = self.algo.load(mock_driver)
        mock_purge_unnamed_unused_columns.assert_called_with(mock_df)
        mock_records_schema.assign_dataframe_names.\
            assert_called_with(include_index=self.mock_dfs_source.include_index, df=mock_df_1)
        mock_df_2.to_sql.assert_called_with(name=self.mock_tbl.table_name,
                                            con=mock_db,
                                            schema=self.mock_tbl.schema_name,
                                            index=self.mock_dfs_source.include_index,
                                            if_exists='append')
        self.assertEqual(out, 3)
