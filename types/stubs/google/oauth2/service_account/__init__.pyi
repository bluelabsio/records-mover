from typing import Mapping, Iterable
import google.auth.service_account

# https://google-auth.readthedocs.io/en/latest/reference/google.oauth2.service_account.html


class Credentials(google.auth.credentials.Credentials):
    @classmethod
    def from_service_account_info(cls, info: Mapping[str, str], scopes: Iterable[str], **kwargs) ->\
            google.auth.service_account.Credentials:
        ...
