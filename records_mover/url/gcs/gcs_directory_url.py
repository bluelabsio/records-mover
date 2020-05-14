from urllib.parse import urlparse, unquote
from records_mover.url import BaseDirectoryUrl, BaseFileUrl
import google.auth.credentials
import google.cloud.storage
import googleapiclient.discovery
from typing import List, Union


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

    def files_and_directories_in_directory(self) -> List[Union[BaseFileUrl, BaseDirectoryUrl]]:
        # https://stackoverflow.com/a/57099354/9795956

        # TODO: Test this manully after creating a directory of a directory
        file_urls = {}
        directory_urls = {}
        service = googleapiclient.discovery.build('storage', 'v1',
                                                  credentials=self.credentials)
        prefix = self.blob
        print(f"PREFIX IS [{prefix}]")
        if prefix == '/':
            # API doesn't seem to recognize the root prefix as anything other than ''
            prefix = ''
        blobs = self.bucket_obj.list_blobs(prefix=prefix, delimiter='/')
        for blob in blobs:
            ...
        raise NotImplementedError()
