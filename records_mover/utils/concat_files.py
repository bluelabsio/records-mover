import io
from typing import IO, List


class ConcatFiles(io.RawIOBase):
    _files: List[IO[bytes]]

    def __init__(self, files: List[IO[bytes]]) -> None:
        self._files = files
        self._tell = 0

    def close(self) -> None:
        for f in self._files:
            f.close()
        self._files = []
        return super().close()

    def readable(self) -> bool:
        return True

    def readall(self) -> bytes:
        # Not sure if Records Mover has any uses of this which may be
        # done on large files, but bytearray() concatentation is
        # suggested by:
        #
        # https://www.guyrutenberg.com/2020/04/04/fast-bytes-concatenation-in-python/
        out = bytearray()
        while self._files:
            f = self._files.pop(0)
            chunk = f.read()
            self._tell += len(chunk)
            out = out + chunk
        return out

    def tell(self) -> int:
        return self._tell

    def read(self, size: int = -1) -> bytes:
        #  "When size is omitted or negative, the entire contents of
        #  the file will be read and returned"
        #
        # https://docs.python.org/3/tutorial/inputoutput.html
        if size < 0:
            return self.readall()

        while len(self._files) > 0:
            chunk = self._files[0].read(size)
            # If we aren't getting any bytes from this stream, lets move on to the next stream
            if len(chunk) == 0:
                f = self._files.pop(0)
                f.close()
            else:
                self._tell += len(chunk)
                return chunk

        return b''
