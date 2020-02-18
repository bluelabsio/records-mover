import boto3
from typing import Optional, List, Any


class S3Concat:
    def __init__(self,
                 bucket: str,
                 key: str,
                 session: boto3.session.Session,
                 min_file_size: Optional[int]):
        ...

    def add_file(self, key: str) -> None:
        ...

    def concat(self) -> List[Any]:
        ...
