import io
from typing import IO, List


class ConcatFiles(io.RawIOBase):
    _files: List[IO[bytes]]

    def __init__(self, files: List[IO[bytes]]) -> None:
        self._files = files
        self._tell = 0
        self._read_started = False

    def close(self) -> None:
        for f in self._files:
            f.close()
        self._files = []
        return super().close()

    def readable(self) -> bool:
        return True

    def readall(self) -> bytes:
        out = b''
        while self._files:
            f = self._files.pop(0)
            out = out + f.read()
        # TODO write a test that forces me to write this code
        # self._tell += len(chunk)
        return out

    def tell(self) -> int:
        return self._tell

    def read(self, size: int = -1) -> bytes:
        #  "When size is omitted or negative, the entire contents of
        #  the file will be read and returned"
        #
        # https://docs.python.org/3/tutorial/inputoutput.html
        if size == -1:
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
