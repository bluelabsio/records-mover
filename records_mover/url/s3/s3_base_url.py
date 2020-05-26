from urllib.parse import urlparse, unquote
import boto3
from botocore.credentials import ReadOnlyCredentials
from typing import TypeVar, Callable, Optional, Any, Union, TYPE_CHECKING
from ..base import BaseDirectoryUrl, BaseFileUrl
if TYPE_CHECKING:
    # http://mypy.readthedocs.io/en/latest/common_issues.html#import-cycles
    from .s3_file_url import S3FileUrl  # noqa
    from .s3_directory_url import S3DirectoryUrl  # noqa

    # These type stubs aren't real classes, so only use their names
    # during type checking
    from boto3.session import S3ResourceTypeStub, S3ClientTypeStub


T = TypeVar('T', bound='S3BaseUrl')


class S3BaseUrl:
    def __init__(self,
                 url: str,
                 boto3_session: boto3.session.Session,
                 S3Url: Callable[[str, Any], Union['S3DirectoryUrl', 'S3FileUrl']]) -> None:
        parsed = urlparse(url)
        self.scheme = parsed.scheme
        self.url = url
        # https://docs.python.org/2/library/urlparse.html
        #
        # "The components are not broken up in smaller parts (for
        # example, the network location is a single string), and %
        # escapes are not expanded."
        self.key = unquote(parsed.path[1:])
        self.bucket = parsed.netloc
        self.region = boto3_session.region_name
        self._boto3_session = boto3_session
        self.s3_resource: S3ResourceTypeStub = boto3_session.resource('s3')
        self.s3_client: S3ClientTypeStub = boto3_session.client('s3')
        self.S3Url = S3Url

    def _directory(self, url: str) -> 'S3DirectoryUrl':
        out = self.S3Url(url, self._boto3_session)
        if not isinstance(out, BaseDirectoryUrl):
            raise TypeError(f"Not a directory url: {url}")
        return out

    def _file(self, url: str) -> 'S3FileUrl':
        out = self.S3Url(url, self._boto3_session)
        if not isinstance(out, BaseFileUrl):
            raise TypeError(f"Not a file url: {url}")
        return out

    def aws_creds(self) -> Optional[ReadOnlyCredentials]:
        """Pull a boto3 credential object that can access this directory.
        Note that these may be expiring and auto-refreshing instance
        creds, so be careful with the result - use the attributes off
        of a single instance rather than calling multiple times, and
        don't save it for later - pull a fresh version when you need
        it.
        """
        creds = self._boto3_session.get_credentials()
        if creds is None:
            return None
        return creds.get_frozen_credentials()

    def containing_directory(self) -> BaseDirectoryUrl:
        parent_url = '/'.join(self.url.split('/')[:-1]) + '/'
        return self._directory(parent_url)

    def _key_in_same_bucket(self, key: str) -> BaseFileUrl:
        return self._file(f"s3://{self.bucket}/{key}")

    def is_directory(self) -> bool:
        return self.url.endswith('/')

    def directory_in_this_directory(self: T, directory_name: str) -> BaseDirectoryUrl:
        raise NotImplementedError()
