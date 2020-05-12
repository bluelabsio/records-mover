from records_mover.records.hints import sniff_hints, sniff_hints_from_fileobjs, sniff_encoding_hint
from records_mover.records import BootstrappingRecordsHints
from mock import MagicMock, patch
import io
import unittest
import json
import os


class TestHints(unittest.TestCase):
    maxDiff = None

    def test_sniff_hints(self):
        resources_dir = os.path.dirname(os.path.abspath(__file__)) + '/../resources'
        hint_sniffing_dir = f"{resources_dir}/hint_sniffing"

        test_cases = [
            os.path.splitext(os.path.basename(f))[0]
            for f in os.listdir(hint_sniffing_dir)
            if (os.path.isfile(os.path.join(hint_sniffing_dir, f)) and
                os.path.splitext(f)[1] == '.csv')
        ]
        for basename in test_cases:
            csv_filename = f'{hint_sniffing_dir}/{basename}.csv'
            config_filename = f'{hint_sniffing_dir}/{basename}.json'
            with open(config_filename, 'r') as config_fileobj:
                config = json.load(config_fileobj)
            required_hints = config['required']
            initial_hints = config['initial_hints']

            with open(csv_filename, 'rb') as fileobj:
                hints = sniff_hints(fileobj, initial_hints=initial_hints)
                self.assertTrue(set(required_hints.items()).issubset(set(hints.items())),
                                f"Expected at least these hints while reading {basename}: "
                                f"{required_hints}, found these hints: {hints}")

    @patch('records_mover.records.hints.csv')
    @patch('records_mover.records.hints.stream_csv')
    @patch('records_mover.records.hints.io')
    def test_sniff_hints_from_fileobjs(self,
                                       mock_io,
                                       mock_stream_csv,
                                       mock_csv) -> None:
        mock_fileobj = MagicMock(name='fileobj')
        mock_fileobj.closed = False
        mock_fileobjs = [mock_fileobj]
        mock_initial_hints: BootstrappingRecordsHints = {
            'field-delimiter': ','
        }
        mock_streaming_engine = mock_stream_csv.return_value.__enter__.return_value._engine
        mock_streaming_engine.compression = 'gzip'
        mock_streaming_engine.encoding = 'utf-8'
        out = sniff_hints_from_fileobjs(fileobjs=mock_fileobjs,
                                        initial_hints=mock_initial_hints)
        self.assertEqual(out, {
            'compression': 'GZIP',
            'dateformat': 'YYYY-MM-DD',
            'datetimeformat': 'YYYY-MM-DD HH:MI:SS',
            'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
            'doublequote': mock_csv.Sniffer().sniff().doublequote,
            'encoding': 'UTF8',
            'quotechar': mock_csv.Sniffer().sniff().quotechar,
            'quoting': 'minimal',
            'field-delimiter': ',',
            'header-row': True,
            'record-terminator': str(mock_io.TextIOWrapper.return_value.newlines),
            'timeonlyformat': 'HH12:MI AM'
        })

    def test_sniff_hints_from_fileobjs_nonseekable(self):
        csv = 'Liberté,égalité,fraternité\n"“a”",2,3\n'
        csv_bytes = csv.encode('utf-8', errors='replace')
        with io.BytesIO(csv_bytes) as fileobj:
            fileobj.seekable = lambda: False
            out = sniff_encoding_hint(fileobj=fileobj)
        self.assertIsNone(out)

    def test_sniff_hints_from_fileobjs_encodings(self):
        expected_hint = {
            'utf-8': {'hint': 'UTF8'},
            'utf-8-sig': {'hint': 'UTF8BOM'},
            # chardet not smart enough right now to sniff UTF16 without BOM:
            # https://github.com/chardet/chardet/pull/109/files
            'utf-16': {'hint': 'UTF16', 'initial': 'UTF16'},
            'utf-16-le': {'hint': 'UTF16LE', 'initial': 'UTF16LE'},
            'utf-16-be': {'hint': 'UTF16BE', 'initial': 'UTF16BE'},
            'latin-1': {'hint': 'LATIN1'},
            'cp1252': {'hint': 'CP1252'},
        }
        for python_encoding, test_details in expected_hint.items():
            csv = 'Liberté,égalité,fraternité\n"“a”",2,3\n'
            csv_bytes = csv.encode(python_encoding, errors='replace')
            with io.BytesIO(csv_bytes) as fileobj:
                fileobjs = [fileobj]
                initial_hints: BootstrappingRecordsHints = {
                    'field-delimiter': ','
                }
                if 'initial' in test_details:
                    # provide some mercy in the form of an initial
                    # hint for things we can't fully sniff yet
                    initial_hints['encoding'] = test_details['initial']
                out = sniff_hints_from_fileobjs(fileobjs=fileobjs,
                                                initial_hints=initial_hints)
            needed_settings = {
                'compression': None,
                'encoding': test_details['hint'],
                'field-delimiter': ',',
                'header-row': True,
                'record-terminator': '\n'
            }
            self.assertTrue(set(needed_settings.items()).issubset(set(out.items())))
