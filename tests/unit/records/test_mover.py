import unittest
from mock import Mock, MagicMock
from records_mover.records.mover import move
from records_mover.records.sources.google_sheets import GoogleSheetsRecordsSource
from records_mover.records.sources.dataframes import DataframesRecordsSource
from records_mover.records.sources.fileobjs import FileobjsSource
from records_mover.records.sources.base import (SupportsMoveToRecordsDirectory)
from records_mover.records.targets.base import (SupportsMoveFromRecordsDirectory,
                                                SupportsMoveFromDataframes,
                                                MightSupportMoveFromFileobjsSource)
import records_mover.records.targets.base as targets
import records_mover.records.sources.base as sources


class TestMover(unittest.TestCase):
    def test_move_from_records_directory_direct(self):
        mock_source = Mock(name='source', spec=sources.SupportsRecordsDirectory)
        mock_source.validate = Mock(name='validate')
        mock_source.records_format = Mock(name='records_format')
        mock_target = Mock(name='target', spec=SupportsMoveFromRecordsDirectory)
        mock_target.validate = Mock(name='validate')
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_target.can_load_direct.return_value = True
        out = move(mock_source, mock_target, mock_processing_instructions)
        mock_target.move_from_records_directory.\
            assert_called_with(directory=mock_source.records_directory.return_value,
                               override_records_format=mock_source.records_format,
                               processing_instructions=mock_processing_instructions)
        mock_scheme = mock_source.records_directory.return_value.loc.scheme
        mock_target.can_load_direct.assert_called_with(mock_scheme)
        self.assertEqual(mock_target.move_from_records_directory.return_value, out)

    def test_move_from_deferred_fileobjs_source(self):
        mock_source = MagicMock(name='source', spec=sources.SupportsToFileobjsSource)
        mock_source.validate = Mock(name='validate')
        mock_target = Mock(name='target', spec=MightSupportMoveFromFileobjsSource)
        mock_target.validate = Mock(name='validate')
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_target.can_move_from_fileobjs_source.return_value = True
        mock_fileobjs_source = MagicMock(name='fileobjs_source', spec=FileobjsSource)
        mock_fileobjs_source.records_format = Mock(name='records_format')
        mock_source.to_fileobjs_source.return_value.__enter__.return_value = mock_fileobjs_source
        out = move(mock_source, mock_target, mock_processing_instructions)
        mock_target.move_from_fileobjs_source.assert_called_with(mock_fileobjs_source,
                                                                 mock_processing_instructions)
        self.assertEqual(mock_target.move_from_fileobjs_source.return_value, out)

    def test_move_from_records_directory_format_compatible(self):
        mock_source = MagicMock(name='source',
                                spec=SupportsMoveToRecordsDirectory)
        mock_source.validate = Mock(name='validate')
        mock_source.records_format = Mock(name='records_format')
        mock_target = Mock(name='target', spec=targets.SupportsRecordsDirectory)
        mock_target.validate = Mock(name='validate')
        mock_target.records_format = Mock(name='records_format')
        mock_source.has_compatible_format.return_value = True
        mock_records_directory = mock_target.records_directory.return_value
        mock_processing_instructions = Mock(name='processing_instructions')
        out = move(mock_source, mock_target, mock_processing_instructions)
        mock_source.move_to_records_directory.\
            assert_called_with(processing_instructions=mock_processing_instructions,
                               records_directory=mock_records_directory,
                               records_format=mock_source.compatible_format.return_value)
        self.assertEqual(mock_source.move_to_records_directory.return_value, out)

    def test_move_from_dataframe(self):
        mock_source = MagicMock(name='source',
                                spec=sources.SupportsToDataframesSource)
        mock_source.validate = Mock(name='validate')
        mock_source.records_format = Mock(name='records_format')
        mock_target = Mock(name='target', spec=SupportsMoveFromDataframes)
        mock_target.validate = Mock(name='validate')
        mock_target.records_format = Mock(name='records_format')
        mock_processing_instructions = Mock(name='processing_instructions')
        out = move(mock_source, mock_target, mock_processing_instructions)
        mock_source.to_dataframes_source.\
            assert_called_with(mock_processing_instructions)
        mock_dfs_source = mock_source.to_dataframes_source.return_value.__enter__.return_value
        mock_target.move_from_dataframes_source.\
            assert_called_with(dfs_source=mock_dfs_source,
                               processing_instructions=mock_processing_instructions)
        self.assertEqual(mock_target.move_from_dataframes_source.return_value, out)

    def test_to_dataframe(self):
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_google_sheets_source = MagicMock(name='google_sheets_source',
                                              spec=GoogleSheetsRecordsSource)
        mock_dataframes_source = MagicMock(name='dataframes_source',
                                           spec=DataframesRecordsSource)
        mock_google_sheets_source.to_dataframes_source.return_value.__enter__.return_value =\
            mock_dataframes_source
        mock_fileobjs_source = MagicMock(name='fileobjs_source',
                                         spec=FileobjsSource)
        mock_dataframes_source.to_fileobjs_source.return_value.__enter__.return_value =\
            mock_fileobjs_source
        mock_target = MagicMock(name='target', spec=targets.SupportsRecordsDirectory)
        mock_directory = mock_target.records_directory.return_value
        mock_google_sheets_source.validate = Mock(name='validate')
        mock_dataframes_source.validate = Mock(name='validate')
        mock_google_sheets_source.validate = Mock(name='validate')
        mock_target.validate = Mock(name='validate')
        out = move(mock_google_sheets_source, mock_target, mock_processing_instructions)
        mock_fileobjs_source.move_to_records_directory.\
            assert_called_with(processing_instructions=mock_processing_instructions,
                               records_directory=mock_directory,
                               records_format=mock_fileobjs_source.compatible_format.return_value)

        self.assertEqual(mock_fileobjs_source.move_to_records_directory.return_value,
                         out)
