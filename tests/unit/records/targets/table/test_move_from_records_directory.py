import unittest
from mock import MagicMock, Mock, patch
from records_mover.records.prep import TablePrep
from records_mover.records.records_directory import RecordsDirectory
from records_mover.records.targets.table.move_from_records_directory import (
    DoMoveFromRecordsDirectory
)


class TestDoMoveFromRecordsDirectory(unittest.TestCase):
    def setUp(self):
        self.mock_prep = Mock(name='prep', spec=TablePrep)
        self.mock_tbl = MagicMock(name='tbl')
        self.mock_directory = Mock(name='directory', spec=RecordsDirectory)
        self.mock_processing_instructions = Mock(name='processing_instructions')
        self.mock_override_records_format = Mock(name='override_records_format')
        self.algo =\
            DoMoveFromRecordsDirectory(prep=self.mock_prep,
                                       target_table_details=self.mock_tbl,
                                       directory=self.mock_directory,
                                       processing_instructions=self.mock_processing_instructions,
                                       override_records_format=self.mock_override_records_format)

    @patch('records_mover.records.targets.table.move_from_records_directory.RecordsLoadPlan')
    def test_move_happy_path(self,
                             mock_RecordsLoadPlan):

        mock_db = self.mock_tbl.db_engine.begin.return_value.__enter__.return_value
        mock_driver = self.mock_tbl.db_driver.return_value
        mock_loader_from_records_directory = mock_driver.loader_from_records_directory.return_value
        mock_records_format = self.mock_override_records_format
        mock_tweaked_records_schema = mock_driver.tweak_records_schema_for_load.return_value
        mock_schema_obj = self.mock_directory.load_schema_json_obj.return_value
        mock_schema_sql = mock_tweaked_records_schema.to_schema_sql.return_value
        mock_import_count = mock_loader_from_records_directory.load.return_value
        mock_plan = mock_RecordsLoadPlan.return_value
        out = self.algo.move()
        self.mock_prep.prep.assert_called_with(schema_sql=mock_schema_sql, driver=mock_driver)
        self.mock_tbl.db_driver.assert_called_with(mock_db)
        mock_driver.tweak_records_schema_for_load.\
            assert_called_with(mock_schema_obj, mock_plan.records_format)
        mock_tweaked_records_schema.to_schema_sql.assert_called_with(mock_driver,
                                                                     self.mock_tbl.schema_name,
                                                                     self.mock_tbl.table_name)
        mock_RecordsLoadPlan.\
            assert_called_with(records_format=mock_records_format,
                               processing_instructions=self.mock_processing_instructions)
        mock_loader_from_records_directory.\
            load.assert_called_with(schema=self.mock_tbl.schema_name,
                                    table=self.mock_tbl.table_name,
                                    load_plan=mock_plan, directory=self.mock_directory)
        self.assertEqual(out.move_count, mock_import_count)

    @patch('records_mover.records.targets.table.move_from_records_directory.RecordsLoadPlan')
    def test_move_legacy_schema_sql(self,
                                    mock_RecordsLoadPlan):
        mock_db = self.mock_tbl.db_engine.begin.return_value.__enter__.return_value
        mock_driver = self.mock_tbl.db_driver.return_value
        mock_loader_from_records_directory = mock_driver.loader_from_records_directory.return_value
        mock_records_format = self.mock_override_records_format
        self.mock_directory.load_schema_json_obj.return_value = None
        mock_schema_sql = self.mock_directory.load_schema_sql_from_sql_file.return_value
        mock_import_count = mock_loader_from_records_directory.load.return_value
        mock_plan = mock_RecordsLoadPlan.return_value
        out = self.algo.move()

        self.mock_prep.prep.assert_called_with(schema_sql=mock_schema_sql, driver=mock_driver)
        self.mock_tbl.db_driver.assert_called_with(mock_db)
        self.mock_directory.load_schema_sql_from_sql_file.assert_called_with()
        mock_RecordsLoadPlan.\
            assert_called_with(records_format=mock_records_format,
                               processing_instructions=self.mock_processing_instructions)
        mock_loader_from_records_directory.\
            load.assert_called_with(schema=self.mock_tbl.schema_name,
                                    table=self.mock_tbl.table_name,
                                    load_plan=mock_plan, directory=self.mock_directory)
        self.assertEqual(out.move_count, mock_import_count)

    @patch('records_mover.records.targets.table.move_from_records_directory.RecordsLoadPlan')
    def test_move_mutant_records_directory(self,
                                           mock_RecordsLoadPlan):
        self.mock_directory.load_schema_json_obj.return_value = None
        self.mock_directory.load_schema_sql_from_sql_file.return_value = None
        with self.assertRaises(SyntaxError):
            self.algo.move()

    @patch('records_mover.records.targets.table.move_from_records_directory.RecordsLoadPlan')
    def test_move_no_override(self,
                              mock_RecordsLoadPlan):
        mock_db = self.mock_tbl.db_engine.begin.return_value.__enter__.return_value
        mock_driver = self.mock_tbl.db_driver.return_value
        mock_loader_from_records_directory = mock_driver.loader_from_records_directory.return_value
        self.mock_directory.load_schema_json_obj.return_value = None
        mock_schema_sql = self.mock_directory.load_schema_sql_from_sql_file.return_value
        mock_import_count = mock_loader_from_records_directory.load.return_value
        mock_plan = mock_RecordsLoadPlan.return_value
        self.algo.override_records_format = None
        mock_records_format = self.mock_directory.load_format.return_value
        out = self.algo.move()

        self.mock_directory.load_format.\
            assert_called_with(self.mock_processing_instructions.fail_if_dont_understand)
        self.mock_prep.prep.assert_called_with(schema_sql=mock_schema_sql, driver=mock_driver)
        self.mock_tbl.db_driver.assert_called_with(mock_db)
        self.mock_directory.load_schema_sql_from_sql_file.assert_called_with()

        mock_RecordsLoadPlan.\
            assert_called_with(records_format=mock_records_format,
                               processing_instructions=self.mock_processing_instructions)
        mock_loader_from_records_directory.load.\
            assert_called_with(schema=self.mock_tbl.schema_name,
                               table=self.mock_tbl.table_name,
                               load_plan=mock_plan, directory=self.mock_directory)
        self.assertEqual(out.move_count, mock_import_count)
