from typing import Iterator, Optional
from google.cloud.storage.blob import Blob


class Bucket:
    def rename_blob(self, blob: Blob, new_name: str) -> Blob:
        ...

    def blob(self, blob_name: str) -> Blob:
        ...
