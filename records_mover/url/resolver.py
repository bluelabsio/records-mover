from urllib.parse import urlparse
from .base import BaseFileUrl, BaseDirectoryUrl
import inspect
from typing import Callable, Optional, Dict, Any, Type, Union, TYPE_CHECKING
if TYPE_CHECKING:
    import google.cloud.storage  # noqa
    import boto3.session  # noqa
import logging


logger = logging.getLogger(__name__)

CreatesUrls = Callable[..., Union[BaseFileUrl, BaseDirectoryUrl]]


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


# TODO: Instead of three functions, how about passing in an object that includes an interface?

class UrlResolver:
    def __init__(self,
                 boto3_session_getter:
                 Callable[[],
                          Optional['boto3.session.Session']],

                 gcs_client_getter:
                 Callable[[],
                          Optional['google.cloud.storage.Client']],

                 gcp_credentials_getter:
                 Callable[[],
                          Optional['google.auth.credentials.Credentials']] = None)\
            -> None:
        self.boto3_session_getter = boto3_session_getter
        self.gcs_client_getter = gcs_client_getter
        self.gcp_credentials_getter = gcp_credentials_getter

    def file_url(self, url: str) -> BaseFileUrl:
        init_urls()
        parsed_url = urlparse(url)
        if parsed_url.scheme in file_url_ctors:
            ctor = file_url_ctors[parsed_url.scheme]

            out = ctor(url, **self.kwargs_for_function(ctor))
            if not isinstance(out, BaseFileUrl):
                raise TypeError(f"Not a file url: {url}")
            return out
        else:
            raise NotImplementedError(f"Teach me how to create FileUrls for {parsed_url.scheme}")

    def kwargs_for_function(self, fn: Callable) -> Dict[str, Any]:
        parameters: Dict[str, Type] = inspect.signature(fn).parameters
        out: Dict[str, Any] = {}
        # TODO: Can I get mypy to complain that I'm assigning a None
        # thing to something that shouldn't ever be None?  Then I can
        # add an error with a good message
        if 'boto3_session' in parameters:
            out["boto3_session"] = self.boto3_session_getter()
        if 'gcs_client' in parameters:
            out["gcs_client"] = self.gcs_client_getter()
        if 'gcp_credentials' in parameters:
            out["gcp_credentials"] = self.gcp_credentials_getter()
        print(f"kwargs: {out}")
        return out

    def directory_url(self, url: str) -> BaseDirectoryUrl:
        init_urls()
        parsed_url = urlparse(url)
        if parsed_url.scheme in directory_url_ctors:
            ctor = directory_url_ctors[parsed_url.scheme]
            out = ctor(url, **self.kwargs_for_function(ctor))
            if not isinstance(out, BaseDirectoryUrl):
                raise TypeError(f"Not a directory url: {url}")
            return out
        else:
            raise NotImplementedError("Teach me how to create DirectoryUrls for "
                                      f"{parsed_url.scheme}")
