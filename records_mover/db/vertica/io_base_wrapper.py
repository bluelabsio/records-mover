from typing import Any
import io


class IOBaseWrapper(io.IOBase):
    def __init__(self, addinfourl_object: Any) -> None:
        self.obj = addinfourl_object

    def read(self, *args, **kwargs):
        return self.obj.read(*args, **kwargs)
