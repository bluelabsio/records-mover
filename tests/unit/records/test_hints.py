from records_mover.records.delimited.sniff import (
    sniff_hints, sniff_hints_from_fileobjs, sniff_encoding_hint, BootstrappingRecordsHints
)
from mock import MagicMock, patch
import io
import gzip
import bz2
import unittest
import json
import os


class TestHints(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.resources_dir = os.path.dirname(os.path.abspath(__file__)) + '/../resources'
        self.hint_sniffing_dir = f"{self.resources_dir}/hint_sniffing"

    def sample_file_basenames(self):
        return [
            os.path.splitext(os.path.basename(f))[0]
            for f in os.listdir(self.hint_sniffing_dir)
            if (os.path.isfile(os.path.join(self.hint_sniffing_dir, f)) and
                os.path.splitext(f)[1] == '.csv')
        ]

    def test_sniff_hints(self):
        for basename in self.sample_file_basenames():
            csv_filename = f'{self.hint_sniffing_dir}/{basename}.csv'
            config_filename = f'{self.hint_sniffing_dir}/{basename}.json'
            with open(config_filename, 'r') as config_fileobj:
                config = json.load(config_fileobj)
            required_hints = config['required']
            initial_hints = config['initial_hints']

            with open(csv_filename, 'rb') as fileobj:
                hints = sniff_hints(fileobj, initial_hints=initial_hints)
                self.assertTrue(set(required_hints.items()).issubset(set(hints.items())),
                                f"Expected at least these hints while reading {basename}: "
                                f"{required_hints}, found these hints: {hints}")

    def test_sniff_hints_gzipped_preinformed(self):
        for basename in self.sample_file_basenames():
            csv_filename = f'{self.hint_sniffing_dir}/{basename}.csv'
            config_filename = f'{self.hint_sniffing_dir}/{basename}.json'
            with open(config_filename, 'r') as config_fileobj:
                config = json.load(config_fileobj)
            required_hints = config['required']
            initial_hints = config['initial_hints']
            required_hints['compression'] = 'GZIP'
            initial_hints['compression'] = 'GZIP'

            with open(csv_filename, 'rb') as uncompressed_fileobj:
                gzipped_data = gzip.compress(uncompressed_fileobj.read())
                fileobj = io.BytesIO(gzipped_data)
                hints = sniff_hints(fileobj, initial_hints=initial_hints)
                self.assertTrue(set(required_hints.items()).issubset(set(hints.items())),
                                f"Expected at least these hints while reading {basename}: "
                                f"{required_hints}, found these hints: {hints}")

    def test_sniff_hints_gzipped_sniffed(self):
        for basename in self.sample_file_basenames():
            csv_filename = f'{self.hint_sniffing_dir}/{basename}.csv'
            config_filename = f'{self.hint_sniffing_dir}/{basename}.json'
            with open(config_filename, 'r') as config_fileobj:
                config = json.load(config_fileobj)
            required_hints = config['required']
            initial_hints = config['initial_hints']
            required_hints['compression'] = 'GZIP'

            with open(csv_filename, 'rb') as uncompressed_fileobj:
                gzipped_data = gzip.compress(uncompressed_fileobj.read())
                fileobj = io.BytesIO(gzipped_data)
                hints = sniff_hints(fileobj, initial_hints=initial_hints)
                self.assertTrue(set(required_hints.items()).issubset(set(hints.items())),
                                f"Expected at least these hints while reading {basename}: "
                                f"{required_hints}, found these hints: {hints}")

    def test_sniff_hints_bzipped_preinformed(self):
        for basename in self.sample_file_basenames():
            csv_filename = f'{self.hint_sniffing_dir}/{basename}.csv'
            config_filename = f'{self.hint_sniffing_dir}/{basename}.json'
            with open(config_filename, 'r') as config_fileobj:
                config = json.load(config_fileobj)
            required_hints = config['required']
            initial_hints = config['initial_hints']
            required_hints['compression'] = 'BZIP'
            initial_hints['compression'] = 'BZIP'

            with open(csv_filename, 'rb') as uncompressed_fileobj:
                gzipped_data = bz2.compress(uncompressed_fileobj.read())
                fileobj = io.BytesIO(gzipped_data)
                hints = sniff_hints(fileobj, initial_hints=initial_hints)
                self.assertTrue(set(required_hints.items()).issubset(set(hints.items())),
                                f"Expected at least these hints while reading {basename}: "
                                f"{required_hints}, found these hints: {hints}")

    def test_sniff_hints_bzipped_sniffed(self):
        for basename in self.sample_file_basenames():
            csv_filename = f'{self.hint_sniffing_dir}/{basename}.csv'
            config_filename = f'{self.hint_sniffing_dir}/{basename}.json'
            with open(config_filename, 'r') as config_fileobj:
                config = json.load(config_fileobj)
            required_hints = config['required']
            initial_hints = config['initial_hints']
            required_hints['compression'] = 'BZIP'

            with open(csv_filename, 'rb') as uncompressed_fileobj:
                gzipped_data = bz2.compress(uncompressed_fileobj.read())
                fileobj = io.BytesIO(gzipped_data)
                hints = sniff_hints(fileobj, initial_hints=initial_hints)
                self.assertTrue(set(required_hints.items()).issubset(set(hints.items())),
                                f"Expected at least these hints while reading {basename}: "
                                f"{required_hints}, found these hints: {hints}")

    # def test_sniff_hints_lzoed_preinformed(self): # TODO
    # def test_sniff_hints_lzoed_sniffed(self): # TODO

    # TODO: https://github.com/ir193/python-lzo/blob/master/lzo.py#L44

    @patch('records_mover.records.delimited.sniff.csv')
    @patch('records_mover.records.delimited.sniff.stream_csv')
    @patch('records_mover.records.delimited.sniff.io')
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
        mock_io.TextIOWrapper.return_value.newlines = '\n'
        mock_streaming_engine.compression = 'gzip'
        mock_streaming_engine.encoding = 'utf-8'
        out = sniff_hints_from_fileobjs(fileobjs=mock_fileobjs,
                                        initial_hints=mock_initial_hints)
        self.assertEqual(out, {
            'compression': 'GZIP',
            'doublequote': mock_csv.Sniffer().sniff().doublequote,
            'encoding': 'UTF8',
            'quotechar': mock_csv.Sniffer().sniff().quotechar,
            'quoting': 'minimal',
            'field-delimiter': ',',
            'header-row': mock_csv.Sniffer().has_header(),
            'record-terminator': str(mock_io.TextIOWrapper.return_value.newlines),
        })

    def test_sniff_hints_from_fileobjs_nonseekable(self):
        csv = 'Liberté,égalité,fraternité\n"“a”",2,3\n'
        csv_bytes = csv.encode('utf-8', errors='replace')
        with io.BytesIO(csv_bytes) as fileobj:
            fileobj.seekable = lambda: False
            with self.assertRaises(OSError):
                sniff_encoding_hint(fileobj=fileobj)

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
            self.assertTrue(set(needed_settings.items()).issubset(set(out.items())),
                            f"Needed at least {needed_settings}, got {out}")
