import google.auth.credentials
from google.cloud.storage.bucket import Bucket
from google.cloud.storage.blob import Blob
from typing import Optional, Iterator


# https://googleapis.dev/python/storage/latest/client.html
class Client:
    def __init__(self,
                 credentials: Optional[google.auth.credentials.Credentials] = None,
                 project: Optional[str] = None):
        ...

    def bucket(self,
               bucket_name: str) -> Bucket:
        ...

    def list_blobs(self,
                   bucket_or_name: str,
                   prefix: Optional[str] = None,
                   delimiter: Optional[str] = None) -> Iterator[Blob]:
        ...
