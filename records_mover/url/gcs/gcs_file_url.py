from urllib.parse import urlparse, unquote
from records_mover.url.gcs.gcs_directory_url import GCSDirectoryUrl
from typing import IO
from records_mover.url import BaseFileUrl
from smart_open.gcs import open as gs_open
import google.cloud.storage


class GCSFileUrl(BaseFileUrl):
    def __init__(self,
                 url: str,
                 gcs_client: google.cloud.storage.Client,
                 gcp_credentials: google.auth.credentials.Credentials,
                 **kwargs) -> None:
        self.url = url
        parsed = urlparse(url)
        self.blob = unquote(parsed.path[1:])
        self.bucket = parsed.netloc
        self.client = gcs_client
        self.credentials = gcp_credentials

    def open(self, mode: str = "rb") -> IO[bytes]:
        return gs_open(bucket_id=self.bucket,
                       blob_id=self.blob,
                       mode=mode,
                       client=self.client)

    def _directory(self, url: str) -> GCSDirectoryUrl:
        return GCSDirectoryUrl(url, gcs_client=self.client, gcp_credentials=self.credentials)
