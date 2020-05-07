import google.auth.credentials
from typing import Optional


class Client:
    def __init__(self,
                 credentials: Optional[google.auth.credentials.Credentials] = None):
        ...
