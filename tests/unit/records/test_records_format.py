import unittest
from records_mover.records.records_format import RecordsFormat, DelimitedRecordsFormat
from records_mover.url.base import BaseDirectoryUrl
from mock import Mock, patch, call
import json


class TestRecordsFormat(unittest.TestCase):
    @patch('records_mover.records.records_format.DelimitedRecordsFormat',
           spec=DelimitedRecordsFormat)
    def test_RecordsFormat_delimited(self,
                                     mock_DelimitedRecordsFormat):
        mock_variant = Mock(name='variant')
        mock_hints = Mock(name='hints')
        mock_processing_instructions = Mock(name='processing_instructions')
        records_format = RecordsFormat(format_type='delimited',
                                       variant=mock_variant,
                                       hints=mock_hints,
                                       processing_instructions=mock_processing_instructions)
        self.assertIsInstance(records_format, DelimitedRecordsFormat)
        mock_DelimitedRecordsFormat.\
            assert_called_with(variant=mock_variant,
                               hints=mock_hints,
                               processing_instructions=mock_processing_instructions)
