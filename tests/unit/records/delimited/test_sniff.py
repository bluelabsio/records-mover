from records_mover.records.delimited.sniff import (
    rewound_fileobj, infer_newline_format, sniff_encoding_hint
)
from mock import Mock, patch
import unittest


class TestSniff(unittest.TestCase):
    def test_rewound_fileobj_already_closed(self):
        mock_fileobj = Mock(name='fileobj')
        mock_fileobj.closed = True
        with self.assertRaises(OSError):
            with rewound_fileobj(mock_fileobj):
                pass

    @patch('records_mover.records.delimited.sniff.io')
    def test_infer_newline_format_cant_infer(self,
                                             mock_io):
        mock_fileobj = Mock(name='fileobj')
        mock_fileobj.closed = False
        mock_encoding_hint = 'UTF8'
        mock_compression = None
        mock_text_fileobj = mock_io.TextIOWrapper.return_value
        mock_text_fileobj.newlines = None
        out = infer_newline_format(mock_fileobj,
                                   mock_encoding_hint,
                                   mock_compression)
        mock_text_fileobj.readline.assert_called
        self.assertIsNone(out)

    @patch('records_mover.records.delimited.sniff.chardet')
    def test_sniff_encoding_hint_no_result(self,
                                           mock_chardet):
        mock_fileobj = Mock(name='fileobj')
        mock_fileobj.closed = False
        mock_chardet.result = {}
        out = sniff_encoding_hint(mock_fileobj)
        self.assertIsNone(out)
