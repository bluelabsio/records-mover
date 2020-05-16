from typing import Iterator, Optional
from google.cloud.storage.blob import Blob


class Bucket:
    def list_blobs(self,
                   prefix: Optional[str] = None,
                   delimiter: Optional[str] = None) -> Iterator[Blob]:
        ...

    def rename_blob(self, blob: Blob, new_name: str) -> Blob:
        ...

    def blob(self, blob_name: str) -> Blob:
        ...
