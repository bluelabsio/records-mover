from google.auth.credentials import Credentials
from typing import Literal


class _ListRequest:
    def execute(self) -> dict:
        ...


class _ObjectsProxy:
    def list(self,
             bucket: str,
             prefix: str,
             delimiter: str) -> _ListRequest:
        ...


class _GCSService:
    def objects(self) -> _ObjectsProxy:
        ...


def build(serviceName: Literal['storage'],
          version: Literal['v1'],
          credentials: Credentials) -> _GCSService:
    ...
