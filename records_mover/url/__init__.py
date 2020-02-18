from urllib.parse import urlparse
from .base import BaseDirectoryUrl, BaseFileUrl
from typing import Dict, Type, Union, Callable, Any


CreatesUrls = Callable[..., Union[BaseFileUrl, BaseDirectoryUrl]]

# These are added as modules are evaluated to avoid circular
# dependencies
directory_url_ctors: Dict[str, Union[Type[BaseDirectoryUrl], CreatesUrls]] = {}
file_url_ctors: Dict[str, Union[Type[BaseFileUrl], CreatesUrls]] = {}


def init_urls() -> None:
    from .s3.s3_url import S3Url
    from .filesystem import FilesystemDirectoryUrl, FilesystemFileUrl
    from .http import HttpFileUrl
    if len(directory_url_ctors) == 0:
        directory_url_ctors['s3'] = S3Url
        directory_url_ctors['file'] = FilesystemDirectoryUrl
    if len(file_url_ctors) == 0:
        file_url_ctors['s3'] = S3Url
        file_url_ctors['file'] = FilesystemFileUrl

        file_url_ctors['http'] = HttpFileUrl
        file_url_ctors['https'] = HttpFileUrl


def FileUrl(url: str, **kwargs: Any) -> BaseFileUrl:
    init_urls()
    parsed_url = urlparse(url)
    if parsed_url.scheme in file_url_ctors:
        ctor = file_url_ctors[parsed_url.scheme]

        out = ctor(url, **kwargs)
        if not isinstance(out, BaseFileUrl):
            raise TypeError(f"Not a file url: {url}")
        return out
    else:
        raise NotImplementedError(f"Teach me how to create FileUrls for {parsed_url.scheme}")


def DirectoryUrl(url: str, **kwargs: Any) -> BaseDirectoryUrl:
    init_urls()
    parsed_url = urlparse(url)
    if parsed_url.scheme in directory_url_ctors:
        ctor = directory_url_ctors[parsed_url.scheme]
        out = ctor(url, **kwargs)
        if not isinstance(out, BaseDirectoryUrl):
            raise TypeError(f"Not a directory url: {url}")
        return out
    else:
        raise NotImplementedError(f"Teach me how to create DirectoryUrls for {parsed_url.scheme}")
