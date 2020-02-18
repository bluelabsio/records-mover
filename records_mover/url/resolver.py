from .base import BaseFileUrl, BaseDirectoryUrl
from . import FileUrl, DirectoryUrl


class UrlResolver:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    def file_url(self, url: str) -> BaseFileUrl:
        return FileUrl(url, **self.kwargs)

    def directory_url(self, url: str) -> BaseDirectoryUrl:
        return DirectoryUrl(url, **self.kwargs)
