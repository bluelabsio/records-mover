from urllib.request import urlopen
from typing import IO


class UrllibFileMixin:
    url: str

    def open(self, mode: str = "rb") -> IO[bytes]:
        if mode != 'rb':
            raise NotImplementedError(f"Mode {mode} not supported on {self.url}")
        return urlopen(self.url)
