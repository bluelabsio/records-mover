from .base_creds import BaseCreds
import logging
from db_facts.db_facts_types import DBFacts
from records_mover.logging import register_secret
from typing import Iterable, Optional, Union, TYPE_CHECKING
if TYPE_CHECKING:
    # see the 'gsheets' extras_require option in setup.py - needed for this!
    import google.auth.credentials  # noqa
    import boto3  # noqa


logger = logging.getLogger(__name__)


class CredsViaAirflow(BaseCreds):
    def boto3_session(self, aws_creds_name: str) -> 'boto3.session.Session':
        from airflow.contrib.hooks.aws_hook import AwsHook
        aws_hook = AwsHook(aws_creds_name)
        return aws_hook.get_session()

    def db_facts(self, db_creds_name: str) -> DBFacts:
        from airflow.hooks import BaseHook
        conn = BaseHook.get_connection(db_creds_name)
        out: DBFacts = {}

        def add(key: str, value: Optional[Union[str, int]]) -> None:
            if value is not None:
                out[key] = value  # type: ignore

        register_secret(conn.password)

        add('host', conn.host)
        add('port', conn.port)
        add('database', conn.schema)
        add('user', conn.login)
        add('password', conn.password)
        # conn.extra_dejson returns {} if no 'extra' is set in the Connection:
        # https://airflow.apache.org/docs/stable/_modules/airflow/models/connection.html
        add('type', conn.extra_dejson.get('type', conn.conn_type.lower()))
        add('bq_default_project_id', conn.extra_dejson.get('extra__google_cloud_platform__project'))
        add('bq_default_dataset_id', conn.extra_dejson.get('bq_default_dataset_id'))
        add('bq_service_account_json',
            conn.extra_dejson.get('extra__google_cloud_platform__keyfile_dict'))
        add('protocol', conn.extra_dejson.get('protocol'))

        return out

    def _gcp_creds(self, gcp_creds_name: str,
                   scopes: Iterable[str]) -> 'google.auth.credentials.Credentials':
        from records_mover.airflow.hooks.google_cloud_credentials_hook import (
            GoogleCloudCredentialsHook
        )
        gcp_hook = GoogleCloudCredentialsHook(gcp_conn_id=gcp_creds_name)
        for intended_scope in scopes:
            if intended_scope not in gcp_hook.scopes():
                logger.warning(f"{intended_scope} not configured as a scope in "
                               f"connection_id {gcp_creds_name}")
        return gcp_hook.get_conn()
