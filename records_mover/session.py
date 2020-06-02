from config_resolver import get_config
from db_facts.db_facts_types import DBFacts
from .creds.base_creds import BaseCreds
from .records.records import Records
from .url.base import BaseFileUrl, BaseDirectoryUrl
from typing import Union, Optional, IO
from .url.resolver import UrlResolver
from records_mover.creds.creds_via_lastpass import CredsViaLastPass
from records_mover.creds.creds_via_airflow import CredsViaAirflow
from records_mover.creds.creds_via_env import CredsViaEnv
from records_mover.logging import set_stream_logging
from records_mover.mover_types import PleaseInfer
import os
import sys
import logging
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .db import DBDriver  # noqa
    from sqlalchemy.engine import Engine, Connection  # noqa
    import boto3  # noqa
    import google.cloud.storage  # noqa


logger = logging.getLogger(__name__)


def _infer_session_type() -> str:
    if 'RECORDS_MOVER_SESSION_TYPE' in os.environ:
        return os.environ['RECORDS_MOVER_SESSION_TYPE']

    config_result = get_config('records_mover', 'bluelabs')
    cfg = config_result.config
    if 'session' in cfg:
        session_cfg = cfg['session']
        session_type: Optional[str] = session_cfg.get('session_type')
        if session_type is not None:
            logger.info(f"Using session_type={session_type} from config file")
            return session_type

    if 'AIRFLOW__CORE__EXECUTOR' in os.environ:
        # Guess based on an env variable sometimes set by Airflow
        return 'airflow'

    return 'env'


def _infer_default_aws_creds_name(session_type: str) -> Optional[str]:
    if session_type == 'airflow':
        return 'aws_default'
    return None


def _infer_default_gcp_creds_name(session_type: str) -> Optional[str]:
    if session_type == 'airflow':
        return 'google_cloud_default'
    return None


def _infer_creds(session_type: str,
                 default_db_creds_name: Optional[str],
                 default_aws_creds_name: Optional[str],
                 default_gcp_creds_name: Optional[str],
                 default_db_facts: Union[PleaseInfer, DBFacts],
                 default_boto3_session: Union[PleaseInfer,
                                              'boto3.session.Session',
                                              None],
                 default_gcp_creds: Union[PleaseInfer,
                                          'google.auth.credentials.Credentials',
                                          None],
                 default_gcs_client: Union[PleaseInfer,
                                           'google.cloud.storage.Client',
                                           None],
                 scratch_s3_url: Union[PleaseInfer,
                                       str,
                                       None]) -> BaseCreds:
    if session_type == 'airflow':
        return CredsViaAirflow(default_db_creds_name=default_db_creds_name,
                               default_aws_creds_name=default_aws_creds_name,
                               default_gcp_creds_name=default_gcp_creds_name,
                               default_db_facts=default_db_facts,
                               default_boto3_session=default_boto3_session,
                               default_gcp_creds=default_gcp_creds,
                               default_gcs_client=default_gcs_client,
                               scratch_s3_url=scratch_s3_url)
    elif session_type == 'cli':
        return CredsViaEnv(default_db_creds_name=default_db_creds_name,
                           default_aws_creds_name=default_aws_creds_name,
                           default_gcp_creds_name=default_gcp_creds_name,
                           default_db_facts=default_db_facts,
                           default_boto3_session=default_boto3_session,
                           default_gcp_creds=default_gcp_creds,
                           default_gcs_client=default_gcs_client)
    elif session_type == 'lpass':
        return CredsViaLastPass(default_db_creds_name=default_db_creds_name,
                                default_aws_creds_name=default_aws_creds_name,
                                default_gcp_creds_name=default_gcp_creds_name,
                                default_db_facts=default_db_facts,
                                default_boto3_session=default_boto3_session,
                                default_gcp_creds=default_gcp_creds,
                                default_gcs_client=default_gcs_client,
                                scratch_s3_url=scratch_s3_url)
    elif session_type == 'itest':
        return CredsViaEnv(default_db_creds_name=default_db_creds_name,
                           default_aws_creds_name=default_aws_creds_name,
                           default_gcp_creds_name=default_gcp_creds_name,
                           default_db_facts=default_db_facts,
                           default_boto3_session=default_boto3_session,
                           default_gcp_creds=default_gcp_creds,
                           default_gcs_client=default_gcs_client,
                           scratch_s3_url=scratch_s3_url)
    elif session_type == 'env':
        return CredsViaEnv(default_db_creds_name=default_db_creds_name,
                           default_aws_creds_name=default_aws_creds_name,
                           default_gcp_creds_name=default_gcp_creds_name,
                           default_db_facts=default_db_facts,
                           default_boto3_session=default_boto3_session,
                           default_gcp_creds=default_gcp_creds,
                           default_gcs_client=default_gcs_client,
                           scratch_s3_url=scratch_s3_url)
    elif session_type is not None:
        raise ValueError("Valid session types: cli, lpass, airflow, itest, env - "
                         "consider upgrading records-mover if you're looking for "
                         f"{session_type}.")


class Session():
    def __init__(self,
                 default_db_creds_name: Optional[str] = None,
                 default_aws_creds_name: Union[None, str, PleaseInfer] = PleaseInfer.token,
                 default_gcp_creds_name: Union[None, str, PleaseInfer] = PleaseInfer.token,
                 session_type: Union[str, PleaseInfer] = PleaseInfer.token,
                 scratch_s3_url: Union[None, str, PleaseInfer] = PleaseInfer.token,
                 creds: Union[BaseCreds, PleaseInfer] = PleaseInfer.token,
                 default_db_facts: Union[PleaseInfer, DBFacts] = PleaseInfer.token,
                 default_boto3_session: Union[PleaseInfer,
                                              'boto3.session.Session',
                                              None] = PleaseInfer.token,
                 default_gcp_creds: Union[PleaseInfer,
                                          'google.auth.credentials.Credentials',
                                          None] = PleaseInfer.token,
                 default_gcs_client: Union[PleaseInfer,
                                           'google.cloud.storage.Client',
                                           None] = PleaseInfer.token) -> None:
        if session_type is PleaseInfer.token:
            session_type = _infer_session_type()

        if default_aws_creds_name is PleaseInfer.token:
            default_aws_creds_name = _infer_default_aws_creds_name(session_type)

        if default_gcp_creds_name is PleaseInfer.token:
            default_gcp_creds_name = _infer_default_gcp_creds_name(session_type)

        if creds is PleaseInfer.token:
            creds = _infer_creds(session_type,
                                 default_db_creds_name=default_db_creds_name,
                                 default_aws_creds_name=default_aws_creds_name,
                                 default_gcp_creds_name=default_gcp_creds_name,
                                 default_db_facts=default_db_facts,
                                 default_boto3_session=default_boto3_session,
                                 default_gcp_creds=default_gcp_creds,
                                 default_gcs_client=default_gcs_client,
                                 scratch_s3_url=scratch_s3_url)

        self.creds = creds

    @property
    def url_resolver(self) -> UrlResolver:
        return UrlResolver(boto3_session_getter=self.creds.default_boto3_session,
                           gcp_credentials_getter=self.creds.default_gcs_creds,
                           gcs_client_getter=self.creds.default_gcs_client)

    def get_default_db_engine(self) -> 'Engine':
        from .db.connect import engine_from_db_facts
        db_facts = self.creds.default_db_facts()

        return engine_from_db_facts(db_facts)

    def get_db_engine(self,
                      db_creds_name: str,
                      creds_provider: Optional[BaseCreds] = None) -> 'Engine':
        from .db.connect import engine_from_db_facts

        if creds_provider is None:
            creds_provider = self.creds
        db_facts = creds_provider.db_facts(db_creds_name)
        return engine_from_db_facts(db_facts)

    def db_driver(self, db: Union['Engine', 'Connection']) -> 'DBDriver':
        from .db.factory import db_driver

        kwargs = {}
        scratch_s3_url = self.creds.default_scratch_s3_url()
        if scratch_s3_url is not None:
            try:
                s3_temp_base_loc = self.directory_url(scratch_s3_url)
                kwargs['s3_temp_base_loc'] = s3_temp_base_loc
            except NotImplementedError:
                logger.debug('boto3 not installed', exc_info=True)

        return db_driver(db=db,
                         url_resolver=self.url_resolver,
                         **kwargs)

    def file_url(self, url: str) -> BaseFileUrl:
        return self.url_resolver.file_url(url)

    def directory_url(self, url: str) -> BaseDirectoryUrl:
        return self.url_resolver.directory_url(url)

    def set_stream_logging(self,
                           name: str = 'records_mover',
                           level: int = logging.INFO,
                           stream: IO[str] = sys.stdout,
                           fmt: str = '%(asctime)s - %(message)s',
                           datefmt: str = '%H:%M:%S') -> None:
        """
        records-mover logs details about its operations using Python logging.  This method is a
        simple way to configure that logging to be output to a stream (by default, stdout).

        You can use it for other things (e.g., dependencies of
        records-mover) by adjusting the 'name' argument.

        :param name: Name of the package to set logging under.  If set
        to 'foo', you can set a log variable FOO_LOG_LEVEL to the log
        level threshold you'd like to set (INFO/WARNING/etc) - so you
        can by default set, say, export
        RECORDS_MOVER_LOG_LEVEL=WARNING to quiet down loging, or
        export RECORDS_MOVER_LOG_LEVEL=DEBUG to increase it.
        :param level: Logging more detailed than this will not be output to the stream.
        :param stream: Stream which logging should be sent (e.g., sys.stdout, sys.stdin, or perhaps
        a file you open)
        :param fmt: Logging format to send to Python'slogging.Formatter() - determines what details
         will be sent.
        :param datefmt: Date format to send to Python'slogging.Formatter() - determines how the
        current date/time will be recorded in the log.
        """
        set_stream_logging(name=name,
                           level=level,
                           stream=stream,
                           fmt=fmt,
                           datefmt=datefmt)

    @property
    def records(self) -> Records:
        return Records(db_driver=self.db_driver,
                       url_resolver=self.url_resolver)
