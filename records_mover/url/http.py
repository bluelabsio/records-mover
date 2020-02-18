from .urllib import UrllibFileMixin
from .base import BaseFileUrl


class HttpFileUrl(UrllibFileMixin, BaseFileUrl):
    def __init__(self, url: str, **kwargs) -> None:
        self.url = url
