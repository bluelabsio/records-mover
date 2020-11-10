from db_facts import db
import os
import base64
import json
from typing import Iterable, Optional
from .base_creds import BaseCreds
from typing import TYPE_CHECKING
from db_facts.db_facts_types import DBFacts
if TYPE_CHECKING:
    # see the 'gsheets' extras_require option in setup.py - needed for this!
    import google.auth.credentials  # noqa
    import boto3  # noqa


class CredsViaEnv(BaseCreds):
    def _gcp_creds_from_env(self,
                            scopes: Iterable[str]) ->\
            Optional['google.auth.credentials.Credentials']:
        if 'GCP_SERVICE_ACCOUNT_JSON_BASE64' not in os.environ:
            return None
        import google.oauth2.service_account
        json_base64 = os.environ['GCP_SERVICE_ACCOUNT_JSON_BASE64']
        json_str = base64.b64decode(json_base64.encode('utf-8'))
        cred_details = json.loads(json_str)

        return google.oauth2.service_account.Credentials.\
            from_service_account_info(cred_details, scopes=scopes)

    def _gcp_creds(self, gcp_creds_name: str,
                   scopes: Iterable[str]) -> 'google.auth.credentials.Credentials':
        # No current way to differentiate between different creds;
        # just on env variable recognized.
        creds_from_env = self._gcp_creds_from_env(scopes)
        if creds_from_env is None:
            raise KeyError('Please set GCP_SERVICE_ACCOUNT_JSON_BASE64 to configure Google '
                           'Cloud Platform credentials')
        return creds_from_env

    def db_facts(self, db_creds_name: str) -> DBFacts:
        return db(db_creds_name.split('-'))

    def _gcp_creds_of_last_resort(self,
                                  scopes: Iterable[str]) ->\
            Optional['google.auth.credentials.Credentials']:
        creds = self._gcp_creds_from_env(scopes)
        if creds is None:
            # Fall back and use Google's default configuration files
            creds = super()._gcp_creds_of_last_resort(scopes=scopes)
        return creds
