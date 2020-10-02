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

    def test_is_readable(self):
        stream = ConcatFiles([io.BytesIO(b'abc'), io.BytesIO(b'abcdef'), io.BytesIO(b'123')])
        self.assertEqual(stream.readable(), True)

    def test_simple_tell(self):
        stream = ConcatFiles([io.BytesIO(b'abc'), io.BytesIO(b'abcdef'), io.BytesIO(b'123')])
        stream.read(3)
        self.assertEqual(3, stream.tell())

    def test_readall_tell(self):
        stream = ConcatFiles([io.BytesIO(b'abc'), io.BytesIO(b'abcdef'), io.BytesIO(b'123')])
        stream.readall()
        self.assertEqual(12, stream.tell())
