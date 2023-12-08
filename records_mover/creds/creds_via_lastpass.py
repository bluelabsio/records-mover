import logging

from db_facts import db
from db_facts.lpass import lpass_field
from db_facts.db_facts_types import DBFacts
import json
from typing import Iterable
from .base_creds import BaseCreds
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    # see the 'gsheets' extras_require option in setup.py - needed for this!
    import google.auth.credentials  # noqa
    import boto3  # noqa


class CredsViaLastPass(BaseCreds):
    def airbyte(self, cred_name: str) -> dict:
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
