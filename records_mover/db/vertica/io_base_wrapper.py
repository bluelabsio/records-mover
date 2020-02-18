from typing import Any
import io


class IOBaseWrapper(io.IOBase):
    def __init__(self, addinfourl_object: Any) -> None:
        self.obj = addinfourl_object
        self.read = self.obj.read
