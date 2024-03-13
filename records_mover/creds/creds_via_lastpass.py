from db_facts import db
from db_facts.lpass import lpass_field
from db_facts.db_facts_types import DBFacts
import json
from typing import Iterable
from .base_creds import BaseCreds
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import google.auth.credentials  # noqa
    import boto3  # noqa


class CredsViaLastPass(BaseCreds):
    def _infer_airbyte_creds(self) -> dict:
        # Magic string! Huzzah. Assumes you have this entry in your local password manager
        cred_name = 'airbyte'
        return {
            'user': lpass_field(cred_name, 'username'),
            'host': lpass_field(cred_name, 'host'),
            'port': int(lpass_field(cred_name, 'port')),
            'endpoint': lpass_field(cred_name, 'endpoint'),
            'password': lpass_field(cred_name, 'password'),
        }

    def _gcp_creds(self, gcp_creds_name: str,
                   scopes: Iterable[str]) -> 'google.auth.credentials.Credentials':
        import google.oauth2.service_account
        notes_json = lpass_field(gcp_creds_name, 'notes')
        cred_details = json.loads(notes_json)

        return google.oauth2.service_account.Credentials.\
            from_service_account_info(cred_details, scopes=scopes)

    def db_facts(self, db_creds_name: str) -> DBFacts:
        return db(db_creds_name.split('-'))

    def boto3_session(self, aws_creds_name: str) -> 'boto3.session.Session':
        import boto3

        if aws_creds_name is None:
            return boto3.session.Session()
        else:
            raise NotImplementedError
