import io
from typing import IO, List


class ConcatFiles(io.RawIOBase):
    _files: List[IO[bytes]]

    def __init__(self, files: List[IO[bytes]]) -> None:
        self._files = files

    def close(self) -> None:
        for f in self._files:
            f.close()
        self._files = []
        return super().close()

    def readable(self) -> bool:
        return True

    def readall(self) -> bytes:
        return b''.join(f.read(-1) for f in self._files)

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            return self.readall()

        while len(self._files) > 0:
            chunk = self._files[0].read(size)
            # If we aren't getting any bytes from this stream, lets move on to the next stream
            if len(chunk) == 0:
                f = self._files.pop(0)
                f.close()
            else:
                return chunk

        return b''
