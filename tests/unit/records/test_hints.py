from records_mover.records.delimited.sniff import (
    sniff_hints_from_fileobjs, PartialRecordsHints
)
from mock import MagicMock, patch
from typing import List, IO
import unittest


class TestHints(unittest.TestCase):
    maxDiff = None

    @patch('records_mover.records.delimited.sniff.csv')
    @patch('records_mover.records.delimited.sniff.stream_csv')
    @patch('records_mover.records.delimited.sniff.io')
    def test_sniff_hints_from_fileobjs(self,
                                       mock_io,
                                       mock_stream_csv,
                                       mock_csv) -> None:
        mock_fileobj = MagicMock(name='fileobj')
        mock_fileobj.closed = False
        mock_fileobjs: List[IO[bytes]] = [mock_fileobj]
        mock_initial_hints: PartialRecordsHints = {
            'field-delimiter': ','
        }
        mock_streaming_engine = mock_stream_csv.return_value.__enter__.return_value._engine
        mock_io.TextIOWrapper.return_value.newlines = '\n'
        mock_streaming_engine.compression = 'gzip'
        mock_streaming_engine.encoding = 'utf-8'
        mock_sniffer = mock_csv.Sniffer.return_value
        mock_sniff_results = mock_sniffer.sniff.return_value
        mock_sniff_results.doublequote = True
        mock_sniffer.has_header.return_value = False
        out = sniff_hints_from_fileobjs(fileobjs=mock_fileobjs,
                                        initial_hints=mock_initial_hints)
        self.assertEqual(out, {
            'compression': 'GZIP',
            'doublequote': True,
            'encoding': 'UTF8',
            'quotechar': mock_csv.Sniffer().sniff().quotechar,
            'quoting': 'minimal',
            'field-delimiter': ',',
            'header-row': False,
            'record-terminator': str(mock_io.TextIOWrapper.return_value.newlines),
        })
