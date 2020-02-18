from records_mover.url.base import blcopyfileobj
import unittest
import io


class TestBase(unittest.TestCase):
    def test_blcopyfileobj(self):
        b = b"some initial binary data: \x00\x01"
        infile = io.BytesIO(b)
        outfile = io.BytesIO(b)
        out = blcopyfileobj(infile, outfile)
        self.assertEqual(out, len(b))
        self.assertEqual(b, outfile.getvalue())
