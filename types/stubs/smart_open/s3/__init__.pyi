# https://github.com/RaRe-Technologies/smart_open/blob/master/smart_open/s3.py
import boto3
from typing import IO, Optional, Dict

DEFAULT_MIN_PART_SIZE = 50 * 1024**2
DEFAULT_BUFFER_SIZE = 128 * 1024


def open(bucket_id: str,
         key_id: str,
         mode: str,
         version_id: Optional[str] = None,
         buffer_size: int = DEFAULT_BUFFER_SIZE,
         min_part_size: int = DEFAULT_MIN_PART_SIZE,
         session: Optional[boto3.session.Session] = None,
         client: Optional[boto3.client] = None,
         resource_kwargs: Optional[dict] = None,
         multipart_upload_kwargs: Optional[Dict] = None) -> IO[bytes]:
    ...
