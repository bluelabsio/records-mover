import google.auth.credentials
from google.cloud.storage.bucket import Bucket
from typing import Optional


# https://googleapis.dev/python/storage/latest/client.html
class Client:
    def __init__(self,
                 credentials: Optional[google.auth.credentials.Credentials] = None):
        ...

    def bucket(self,
               bucket_name: str) -> Bucket:
        ...
