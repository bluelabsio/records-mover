from .base import BaseFileUrl, BaseDirectoryUrl
from . import FileUrl, DirectoryUrl
from typing import Callable, Optional, TYPE_CHECKING
if TYPE_CHECKING:
    import google.cloud.storage  # noqa
    import boto3.session  # noqa


class UrlResolver:
    def __init__(self,
                 boto3_session_getter: Callable[[],
                                                Optional['boto3.session.Session']],
                 gcs_client_getter: Callable[[],
                                             Optional['google.cloud.storage.Client']],
                 gcp_credentials_getter: Callable[[],
                                                  Optional['google.auth.credentials.Credentials']])\
            -> None:
        self.boto3_session_getter = boto3_session_getter
        self.gcs_client_getter = gcs_client_getter
        self.gcp_credentials_getter = gcp_credentials_getter

    def file_url(self, url: str) -> BaseFileUrl:
        return FileUrl(url,
                       boto3_session=self.boto3_session_getter(),
                       gcs_client=self.gcs_client_getter(),
                       gcp_credentials=self.gcp_credentials_getter())

    def directory_url(self, url: str) -> BaseDirectoryUrl:
        return DirectoryUrl(url,
                            boto3_session=self.boto3_session_getter(),
                            gcs_client=self.gcs_client_getter(),
                            gcp_credentials=self.gcp_credentials_getter())
