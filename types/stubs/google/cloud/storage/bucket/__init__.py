from google.cloud.storage.blob import Blob


class Bucket:
    def rename_blob(self, blob: Blob, new_name: str) -> Blob:  # type: ignore
        ...

    def blob(self, blob_name: str) -> Blob:  # type: ignore
        ...
