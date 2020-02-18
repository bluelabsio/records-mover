# https://github.com/boto/botocore/blob/9f5afe26cc4a2695dcb62db453f9db7c299f3ffc/botocore/credentials.py
from typing import NamedTuple

ReadOnlyCredentials = NamedTuple('ReadOnlyCredentials',
                                 [('access_key', str),
                                  ('secret_key', str),
                                  ('token', str)])


class Credentials:
    access_key: str
    secret_key: str
    token: str

    def get_frozen_credentials(self) -> ReadOnlyCredentials:
        ...
