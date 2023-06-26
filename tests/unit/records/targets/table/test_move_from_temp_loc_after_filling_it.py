import unittest
from mock import MagicMock, Mock, patch
from records_mover.records.prep import TablePrep
from records_mover.records.targets.table.move_from_temp_loc_after_filling_it import (
    DoMoveFromTempLocAfterFillingIt
)


class TestDoMoveFromTempLocAfterFillingIt(unittest.TestCase):
    @patch('records_mover.records.targets.table.move_from_temp_loc_after_filling_it.' +
           'RecordsDirectory')
    def test_init(self,
                  mock_RecordsDirectory):
        mock_prep = Mock(name='prep', spec=TablePrep)
        mock_tbl = MagicMock(name='tbl')
        mock_table_target = Mock(name='table_target')
        mock_records_source = Mock(name='records_source')
        mock_processing_instructions = Mock(name='processing_instructions',
                                            spec='ProcessingInstructions')
        algo = DoMoveFromTempLocAfterFillingIt(prep=mock_prep,
                                               target_table_details=mock_tbl,
                                               table_target=mock_table_target,
                                               records_source=mock_records_source,
                                               processing_instructions=mock_processing_instructions)
        mock_records_format = mock_records_source.compatible_format.return_value
        mock_pis = mock_processing_instructions
        mock_driver = mock_tbl.db_driver.return_value
        mock_loader = mock_driver.loader.return_value
        mock_temp_loc =\
            mock_loader.temporary_loadable_directory_loc.return_value.__enter__.return_value
        mock_directory = mock_RecordsDirectory.return_value
        out = algo.move()
        mock_records_source.compatible_format.assert_called_with(mock_table_target)
        mock_tbl.db_driver.assert_called_with(db=None, db_engine=mock_tbl.db_engine)
        mock_RecordsDirectory.assert_called_with(records_loc=mock_temp_loc)
        mock_records_source.move_to_records_directory.\
            assert_called_with(records_directory=mock_directory,
                               records_format=mock_records_format,
                               processing_instructions=mock_pis)
        mock_table_target.move_from_records_directory.\
            assert_called_with(directory=mock_directory,
                               processing_instructions=mock_processing_instructions)
        self.assertEqual(out, mock_table_target.move_from_records_directory.return_value)
