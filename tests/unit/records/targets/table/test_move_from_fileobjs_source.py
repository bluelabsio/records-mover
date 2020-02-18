import unittest
from mock import MagicMock, Mock, patch, call
from records_mover.records.prep import TablePrep
from records_mover.records.schema import RecordsSchema
from records_mover.records.records_format import RecordsFormat
from records_mover.records.existing_table_handling import ExistingTableHandling
from records_mover.records.sources.fileobjs import FileobjsSource
from records_mover.records.targets.table.move_from_fileobjs_source import (
    DoMoveFromFileobjsSource
)


class TestDoMoveFromFileobjsSource(unittest.TestCase):
    @patch('records_mover.records.targets.table.move_from_fileobjs_source.RecordsLoadPlan')
    def setUp(self, mock_RecordsLoadPlan):
        self.mock_RecordsLoadPlan = mock_RecordsLoadPlan
        self.mock_prep = Mock(name='prep', spec=TablePrep)
        self.mock_tbl = MagicMock(name='tbl')
        self.mock_processing_instructions = Mock(name='processing_instructions',
                                                 spec='ProcessingInstructions')
        self.mock_fileobjs_source = Mock(name='fileobjs_source', spec=FileobjsSource)
        self.mock_fileobjs_source.records_schema = Mock(name='record_schema', spec=RecordsSchema)
        self.mock_fileobjs_source.records_format = Mock(name='record_format', spec=RecordsFormat)
        mock_fileobj_a = Mock(name='fileobj_a')
        self.mock_fileobjs_source.target_names_to_input_fileobjs = {
            'fileobj_a': mock_fileobj_a
        }
        self.mock_fileobj = mock_fileobj_a
        self.mock_plan = self.mock_RecordsLoadPlan.return_value
        self.algo =\
            DoMoveFromFileobjsSource(prep=self.mock_prep,
                                     target_table_details=self.mock_tbl,
                                     fileobjs_source=self.mock_fileobjs_source,
                                     processing_instructions=self.mock_processing_instructions)

    def test_move(self):
        mock_db = self.mock_tbl.db_engine.begin.return_value.__enter__.return_value
        mock_driver = self.mock_tbl.db_driver.return_value
        mock_tweaked_records_schema = mock_driver.tweak_records_schema_for_load.return_value
        mock_schema_sql = mock_tweaked_records_schema.to_schema_sql.return_value
        mock_import_count = mock_driver.load_from_fileobj.return_value
        out = self.algo.move()
        self.mock_tbl.db_driver.assert_called_with(mock_db)
        self.mock_RecordsLoadPlan.\
            assert_called_with(records_format=self.mock_fileobjs_source.records_format,
                               processing_instructions=self.mock_processing_instructions)
        self.mock_prep.prep.assert_called_with(schema_sql=mock_schema_sql,
                                               driver=mock_driver)
        mock_driver.load_from_fileobj.assert_called_with(schema=self.mock_tbl.schema_name,
                                                         table=self.mock_tbl.table_name,
                                                         load_plan=self.mock_plan,
                                                         fileobj=self.mock_fileobj)
        mock_tweaked_records_schema.to_schema_sql.\
            assert_called_with(mock_driver,
                               self.mock_tbl.schema_name,
                               self.mock_tbl.table_name)
        self.assertEqual(out.move_count, mock_import_count)

    def test_move_with_load_failure_dont_recreate_reraise(self):
        class MyException(Exception):
            pass

        mock_db = self.mock_tbl.db_engine.begin.return_value.__enter__.return_value
        mock_driver = self.mock_tbl.db_driver.return_value
        mock_tweaked_records_schema = mock_driver.tweak_records_schema_for_load.return_value
        mock_schema_sql = mock_tweaked_records_schema.to_schema_sql.return_value
        mock_driver.load_from_fileobj.side_effect = MyException
        mock_driver.load_failure_exception.return_value = MyException
        self.mock_tbl.drop_and_recreate_on_load_error = False
        with self.assertRaises(MyException):
            self.algo.move()

        mock_driver.load_failure_exception.assert_called_with()

        self.mock_tbl.db_driver.assert_called_with(mock_db)
        mock_tweaked_records_schema.to_schema_sql.\
            assert_called_with(mock_driver,
                               self.mock_tbl.schema_name,
                               self.mock_tbl.table_name)
        self.mock_RecordsLoadPlan.\
            assert_called_with(records_format=self.mock_fileobjs_source.records_format,
                               processing_instructions=self.mock_processing_instructions)
        self.mock_prep.prep.assert_called_with(schema_sql=mock_schema_sql,
                                               driver=mock_driver)
        mock_driver.load_from_fileobj.assert_called_with(schema=self.mock_tbl.schema_name,
                                                         table=self.mock_tbl.table_name,
                                                         load_plan=self.mock_plan,
                                                         fileobj=self.mock_fileobj)

    def test_move_with_load_failure_recreate(self):
        class MyException(Exception):
            pass

        mock_db = self.mock_tbl.db_engine.begin.return_value.__enter__.return_value
        mock_driver = self.mock_tbl.db_driver.return_value
        mock_tweaked_records_schema = mock_driver.tweak_records_schema_for_load.return_value
        mock_schema_sql = mock_tweaked_records_schema.to_schema_sql.return_value
        mock_plan = self.mock_RecordsLoadPlan.return_value
        mock_import_count = Mock(name='import_count')
        mock_driver.load_from_fileobj.side_effect = [
            MyException,
            mock_import_count,
        ]
        mock_driver.load_failure_exception.return_value = MyException
        self.mock_tbl.drop_and_recreate_on_load_error = True
        self.mock_fileobj.seekable.return_value = True
        out = self.algo.move()

        mock_driver.load_failure_exception.assert_called_with()

        self.mock_tbl.db_driver.assert_called_with(mock_db)
        mock_tweaked_records_schema.to_schema_sql.\
            assert_called_with(mock_driver,
                               self.mock_tbl.schema_name,
                               self.mock_tbl.table_name)
        self.mock_RecordsLoadPlan.\
            assert_called_with(records_format=self.mock_fileobjs_source.records_format,
                               processing_instructions=self.mock_processing_instructions)
        self.mock_prep.prep.assert_has_calls([
            call(schema_sql=mock_schema_sql, driver=mock_driver),
            call(schema_sql=mock_schema_sql,
                 existing_table_handling=ExistingTableHandling.DROP_AND_RECREATE,
                 driver=mock_driver),
        ])
        self.mock_fileobj.seekable.assert_called_with()
        self.mock_fileobj.seek.assert_called_with(0)
        mock_driver.load_from_fileobj.assert_called_with(schema=self.mock_tbl.schema_name,
                                                         table=self.mock_tbl.table_name,
                                                         load_plan=mock_plan,
                                                         fileobj=self.mock_fileobj)
        self.assertEqual(out.move_count, mock_import_count)
