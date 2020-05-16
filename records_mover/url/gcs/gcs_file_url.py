from urllib.parse import urlparse, unquote
from records_mover.url.gcs.gcs_directory_url import GCSDirectoryUrl
from typing import IO
from records_mover.url import BaseFileUrl
from smart_open.gcs import open as gs_open
import google.cloud.storage
from google.cloud.storage.blob import Blob
import google.api_core.exceptions


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
        self.bucket_obj = self.client.bucket(self.bucket)

    def open(self, mode: str = "rb") -> IO[bytes]:
        try:
            return gs_open(bucket_id=self.bucket,
                           blob_id=self.blob,
                           mode=mode,
                           client=self.client)
        except google.api_core.exceptions.NotFound:
            # google.api_core.exceptions.NotFound: 404 blob
            # SkosHyDshbo/_manifest not found in bucket-name-here
            #
            # smart-open version: 2.0
            raise FileNotFoundError(f"{self} not found")

    def _directory(self, url: str) -> GCSDirectoryUrl:
        return GCSDirectoryUrl(url, gcs_client=self.client, gcp_credentials=self.credentials)

    def _blob_obj(self) -> 'Blob':
        return self.bucket_obj.blob(self.blob)

    def size(self) -> int:
        return self._blob_obj().size

    def rename_to(self, new: 'BaseFileUrl') -> 'BaseFileUrl':
        if not isinstance(new, GCSFileUrl):
            raise NotImplementedError('Cannot rename a GCS file to a non-GCS file')
        if new.bucket != self.bucket:
            raise NotImplementedError('Cannot rename a GCS file in a different bucket')
        # This actually does a copy behind the scenes, so don't get
        # too excited:
        self.bucket_obj.rename_blob(self._blob_obj(), new.blob)

        return new

    def filename(self) -> str:
        return self.url[self.url.rfind("/")+1:]

    def delete(self) -> None:
        self._blob_obj().delete()
