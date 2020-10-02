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

    def test_read_past_one_file_marker(self):
        # Read up to size bytes from the object and return them.  As a
        # convenience, if size is unspecified or -1, all bytes until
        # EOF are returned. Otherwise, only one system call is ever
        # made. Fewer than size bytes may be returned if the operating
        # system call returns fewer than size bytes.
        #
        # https://docs.python.org/3/library/io.html#io.RawIOBase.read
        stream = ConcatFiles([io.BytesIO(b'abc'), io.BytesIO(b'abcdef'), io.BytesIO(b'123')])
        chunk = stream.read(6)
        # Ensure at most one read system call is potentially made
        # against streams inside.
        self.assertEqual(chunk, b'abc')
