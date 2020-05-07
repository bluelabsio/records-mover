from urllib.parse import urlparse, unquote
from typing import IO
from records_mover.url import BaseFileUrl
from smart_open.gcs import open as gs_open
import google.cloud.storage


class GSFileUrl(BaseFileUrl):
    client: google.cloud.storage.Client

    def __init__(self,
                 url: str,
                 gcs_client: google.cloud.storage.Client,
                 **kwargs) -> None:
        self.url = url
        parsed = urlparse(url)
        self.blob = unquote(parsed.path[1:])
        self.bucket = parsed.netloc
        self.client = gcs_client

    def open(self, mode: str = "rb") -> IO[bytes]:
        return gs_open(bucket_id=self.bucket,
                       blob_id=self.blob,
                       mode=mode,
                       client=self.client)
