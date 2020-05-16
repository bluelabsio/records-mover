from urllib.parse import urlparse, unquote
from records_mover.url import BaseDirectoryUrl, BaseFileUrl
import google.auth.credentials
import google.cloud.storage
import googleapiclient.discovery
from typing import List, Union, TYPE_CHECKING
if TYPE_CHECKING:
    from .gcs_file_url import GCSFileUrl


class GCSDirectoryUrl(BaseDirectoryUrl):
    def __init__(self,
                 url: str,
                 gcs_client: google.cloud.storage.Client,
                 gcp_credentials: google.auth.credentials.Credentials,
                 **kwargs) -> None:
        self.url = url
        parsed = urlparse(url)
        self.blob = unquote(parsed.path[1:])
        self.scheme = parsed.scheme
        self.bucket = parsed.netloc
        self.client = gcs_client
        self.credentials = gcp_credentials
        self.bucket_obj = self.client.bucket(self.bucket)

    def _directory(self, url: str) -> 'GCSDirectoryUrl':
        return GCSDirectoryUrl(url, gcs_client=self.client, gcp_credentials=self.credentials)

    def _file(self, url: str) -> 'GCSFileUrl':
        from .gcs_file_url import GCSFileUrl
        return GCSFileUrl(url, gcs_client=self.client, gcp_credentials=self.credentials)

    def directory_in_this_directory(self, directory_name: str) -> 'GCSDirectoryUrl':
        return self._directory(f"{self.url}{directory_name}/")

    def purge_directory(self) -> None:
        if not self.is_directory():
            raise ValueError("Not a directory")
        # https://stackoverflow.com/questions/53165246/how-to-delete-gcs-folder-from-python
        # TODO: see deprecation - https://googleapis.dev/python/storage/latest/buckets.html#google.cloud.storage.bucket.Bucket.list_blobs
        blobs = self.bucket_obj.list_blobs(prefix=self.blob)
        for blob in blobs:
            # TODO: Does this work recursively?
            blob.delete()

    def files_in_directory(self) -> List[BaseFileUrl]:
        prefix = self.blob
        if prefix == '/':
            # API doesn't seem to recognize the root prefix as anything other than ''
            prefix = ''

        blobs = self.bucket_obj.list_blobs(prefix=prefix, delimiter='/')
        blob_names = [
            blob.name
            for blob in blobs
            # I've seen this happen with gs://bluelabs-test-recordsmover/bar/
            if blob.name != prefix
        ]
        file_urls = [
            f"gs://{self.bucket}/{blob_name}"
            for blob_name in blob_names
        ]
        return [self._file(file_url) for file_url in file_urls]

    def directories_in_directory(self) -> List[BaseDirectoryUrl]:
        # https://stackoverflow.com/a/57099354/9795956

        service = googleapiclient.discovery.build('storage', 'v1',
                                                  credentials=self.credentials)
        prefix = self.blob
        if prefix == '/':
            # API doesn't seem to recognize the root prefix as anything other than ''
            prefix = ''
        folders_req = service.objects().list(bucket=self.bucket,
                                             prefix=prefix,
                                             delimiter='/')
        folders_resp = folders_req.execute()
        folder_names = folders_resp.get('prefixes', [])
        directory_urls = [
            f"gs://{self.bucket}/{folder_name}"
            for folder_name in folder_names
            # I haven't seen this happen - this is for safety
            if folder_name != prefix
        ]
        return [self._directory(directory_url) for directory_url in directory_urls]
