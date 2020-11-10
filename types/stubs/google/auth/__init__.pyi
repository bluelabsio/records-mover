from google.auth.credentials import Credentials
from typing import Tuple, Iterable


def default(scopes: Iterable[str]) -> Tuple[Credentials, str]:
    ...
