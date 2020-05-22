from .base import BaseDirectoryUrl, BaseFileUrl
import logging
from typing import Dict, Type, Union, Callable

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
