from urllib.parse import urlparse
from .base import BaseFileUrl, BaseDirectoryUrl
import inspect
# TODO: Should I move these data structures here?
from records_mover.url import init_urls, file_url_ctors, directory_url_ctors
from typing import Callable, Optional, Dict, Any, Type, TYPE_CHECKING
if TYPE_CHECKING:
    import google.cloud.storage  # noqa
    import boto3.session  # noqa


class UrlResolver:
    def __init__(self,
                 boto3_session_getter:
                 Callable[[],
                          Optional['boto3.session.Session']],

                 gcs_client_getter:
                 Optional[Callable[[],
                                   Optional['google.cloud.storage.Client']]] = None,

                 gcp_credentials_getter:
                 Optional[Callable[[],
                                   Optional['google.auth.credentials.Credentials']]] = None)\
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
        # TODO: Why shouldn't boto3_session_getter also be nullable
        if 'boto3_session' in parameters:
            out["boto3_session"] = self.boto3_session_getter()
        if 'gcs_client' in parameters and self.gcs_client_getter is not None:
            out["gcs_client"] = self.gcs_client_getter()
        if 'gcp_credentials' in parameters and self.gcp_credentials_getter is not None:
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
