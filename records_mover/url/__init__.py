from urllib.parse import urlparse
from .base import BaseDirectoryUrl, BaseFileUrl
import logging
from typing import Dict, Type, Union, Callable, Any

CreatesUrls = Callable[..., Union[BaseFileUrl, BaseDirectoryUrl]]

logger = logging.getLogger(__name__)

# These are added as modules are evaluated to avoid circular
# dependencies
directory_url_ctors: Dict[str, Union[Type[BaseDirectoryUrl], CreatesUrls]] = {}
file_url_ctors: Dict[str, Union[Type[BaseFileUrl], CreatesUrls]] = {}


def init_urls() -> None:
    try:
        from .s3.s3_url import S3Url
    except ModuleNotFoundError:
        logger.debug('No S3 support', exc_info=True)
        S3Url = None  # type: ignore
    try:
        from .gcs.gcs_file_url import GCSFileUrl
        from .gcs.gcs_directory_url import GCSDirectoryUrl
    except ModuleNotFoundError:
        logger.debug('No Google Cloud Storage support', exc_info=True)
        GCSFileUrl = None  # type: ignore
        GCSDirectoryUrl = None  # type: ignore
    from .filesystem import FilesystemDirectoryUrl, FilesystemFileUrl
    from .http import HttpFileUrl
    if len(directory_url_ctors) == 0:
        if S3Url is not None:
            directory_url_ctors['s3'] = S3Url
        if GCSDirectoryUrl is not None:
            directory_url_ctors['gs'] = GCSDirectoryUrl
        directory_url_ctors['file'] = FilesystemDirectoryUrl
    if len(file_url_ctors) == 0:
        if S3Url is not None:
            file_url_ctors['s3'] = S3Url
        if GCSFileUrl is not None:
            file_url_ctors['gs'] = GCSFileUrl
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
