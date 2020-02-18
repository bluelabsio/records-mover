from .base_creds import BaseCreds
import boto3
import logging
from db_facts.db_facts_types import DBFacts
from typing import Iterable, TYPE_CHECKING
if TYPE_CHECKING:
    # see the 'gsheets' extras_require option in setup.py - needed for this!
    import google.auth.credentials  # noqa

logger = logging.getLogger(__name__)


class CredsViaAirflow(BaseCreds):
    def boto3_session(self, aws_creds_name: str) -> boto3.session.Session:
        from airflow.contrib.hooks.aws_hook import AwsHook
        aws_hook = AwsHook(aws_creds_name)
        return aws_hook.get_session()

    def db_facts(self, db_creds_name: str) -> DBFacts:
        from airflow.hooks import BaseHook
        conn = BaseHook.get_connection(db_creds_name)
        return {
            'host': conn.host,
            'port': conn.port,
            'database': conn.schema,
            'user': conn.login,
            'password': conn.password,
            'type': conn.extra_dejson.get('type', conn.conn_type.lower()),
        }

    def _gcp_creds(self, gcp_creds_name: str,
                   scopes: Iterable[str]) -> 'google.auth.credentials.Credentials':
        from records_mover.airflow.google_cloud_credentials_hook import GoogleCloudCredentialsHook
        gcp_hook = GoogleCloudCredentialsHook(gcp_conn_id=gcp_creds_name)
        for intended_scope in scopes:
            if intended_scope not in gcp_hook.scopes():
                logger.warning(f"{intended_scope} not configured as a scope in "
                               f"connection_id {gcp_creds_name}")
        return gcp_hook.get_conn()
