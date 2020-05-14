from urllib.parse import urlparse, unquote
from records_mover.url import BaseDirectoryUrl
import google.cloud.storage


class GCSDirectoryUrl(BaseDirectoryUrl):
    def __init__(self,
                 url: str,
                 gcs_client: google.cloud.storage.Client,
                 **kwargs) -> None:
        self.url = url
        parsed = urlparse(url)
        self.blob = unquote(parsed.path[1:])
        self.scheme = parsed.scheme
        self.bucket = parsed.netloc
        self.client = gcs_client
        self.bucket_obj = self.client.bucket(self.bucket)

    def _directory(self, url: str) -> 'GCSDirectoryUrl':
        return GCSDirectoryUrl(url, gcs_client=self.client)

    def directory_in_this_directory(self, directory_name: str) -> 'GCSDirectoryUrl':
        return self._directory(f"{self.url}{directory_name}/")

    def purge_directory(self) -> None:
        if not self.is_directory():
            raise ValueError("Not a directory")
        # https://stackoverflow.com/questions/53165246/how-to-delete-gcs-folder-from-python
        blobs = self.bucket_obj.list_blobs(prefix=self.blob)
        for blob in blobs:
            blob.delete()
