import io
import unittest

from records_mover.utils.concat_files import ConcatFiles


class TestConcatFiles(unittest.TestCase):
    def test_read_reads_in_chunks(self):
        stream = ConcatFiles([io.BytesIO(b'abc'), io.BytesIO(b'abcdef'), io.BytesIO(b'123')])

        chunks = iter(lambda: stream.read(6), b'')
        self.assertEqual(b''.join(chunks), b'abcabcdef123')

    def test_read_minus_one(self):
        stream = ConcatFiles([io.BytesIO(b'abc'), io.BytesIO(b'abcdef'), io.BytesIO(b'123')])
        self.assertEqual(stream.read(-1), b'abcabcdef123')
