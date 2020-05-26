# https://github.com/RaRe-Technologies/smart_open/blob/master/smart_open/gcs.py
import google.cloud.storage
from typing import IO, Optional

_MIN_MIN_PART_SIZE = _REQUIRED_CHUNK_MULTIPLE = 256 * 1024

DEFAULT_BUFFER_SIZE = 256 * 1024
"""Default buffer size for working with GCS"""


def open(bucket_id: str,
         blob_id: str,
         mode: str,
         buffer_size: int = DEFAULT_BUFFER_SIZE,
         min_part_size: int = _MIN_MIN_PART_SIZE,
         client: Optional[google.cloud.storage.Client] = None) -> IO[bytes]:
    ...
