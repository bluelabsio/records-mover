from db_facts import db
from .lpass import lpass_field
import json
from typing import Iterable
from .base_creds import BaseCreds
from typing import TYPE_CHECKING
from db_facts.db_facts_types import DBFacts
if TYPE_CHECKING:
    # see the 'gsheets' extras_require option in setup.py - needed for this!
    import google.auth.credentials  # noqa
    import boto3  # noqa


class CredsViaLastPass(BaseCreds):
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
