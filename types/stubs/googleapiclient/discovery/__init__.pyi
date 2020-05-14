from google.auth.credentials import Credentials
from typing import Literal

class _GCSService:
    ...


def build(serviceName: Literal['storage'],
          version: Literal['v1'],
          credentials: Credentials) -> _GCSService:
    ...
