from .creds.base_creds import BaseCreds
from .database import db_facts_from_env
from .records.records import Records
from .url.base import BaseFileUrl, BaseDirectoryUrl
from typing import Union, Optional, IO
from .url.resolver import UrlResolver
from db_facts.db_facts_types import DBFacts
from enum import Enum
from records_mover.creds.creds_via_lastpass import CredsViaLastPass
from records_mover.creds.creds_via_airflow import CredsViaAirflow
from records_mover.creds.creds_via_env import CredsViaEnv
from records_mover.logging import set_stream_logging
from config_resolver import get_config
import subprocess
import os
import sys
import logging
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .db import DBDriver  # noqa
    from sqlalchemy.engine import Engine, Connection  # noqa
    import boto3  # noqa


logger = logging.getLogger(__name__)


def _infer_session_type() -> str:
    if 'RECORDS_MOVER_SESSION_TYPE' in os.environ:
        return os.environ['RECORDS_MOVER_SESSION_TYPE']

    if 'AIRFLOW__CORE__EXECUTOR' in os.environ:
        # Guess based on an env variable sometimes set by Airflow
        return 'airflow'

    return 'cli'


def _infer_scratch_s3_url(session_type: str) -> Optional[str]:
    if "SCRATCH_S3_URL" in os.environ:
        return os.environ["SCRATCH_S3_URL"]

    # config_resolver logs at the WARNING level for each time it
    # attempts to load a config file and doesn't find it - which given
    # it searches a variety of places, is quite noisy.
    #
    # https://github.com/exhuma/config_resolver/blob/master/doc/intro.rst#logging
    #
    # https://github.com/exhuma/config_resolver/issues/69
    logging.getLogger('config_resolver').setLevel(logging.ERROR)
    config_result = get_config('records_mover', 'bluelabs')
    cfg = config_result.config
    if 'aws' in cfg:
        s3_scratch_url: Optional[str] = cfg['aws'].get('s3_scratch_url')
        if s3_scratch_url is not None:
            return s3_scratch_url
    else:
        logger.debug('No config ini file found')

    if session_type == 'cli':
        try:
            #
            # https://app.asana.com/0/1128138765527694/1163219515343393
            #
            # This method of configuration needs to be replaced with
            # something more conventional and documented.
            #
            return subprocess.check_output("scratch-s3-url").decode('ASCII').rstrip()
        except FileNotFoundError:
            pass
    return None


def _infer_default_aws_creds_name(session_type: str) -> Optional[str]:
    if session_type == 'airflow':
        return 'aws_default'
    return None


def _infer_creds(session_type: str) -> BaseCreds:
    if session_type == 'airflow':
        return CredsViaAirflow()
    elif session_type == 'cli':
        #
        # https://app.asana.com/0/1128138765527694/1163219515343393
        #
        # Most people don't use LastPass; other secrets managements
        # should be supported and configurable at the system- and
        # user- level.
        #
        return CredsViaLastPass()
    elif session_type == 'itest':
        return CredsViaEnv()
    elif session_type == 'env':
        return CredsViaEnv()
    elif session_type is not None:
        raise ValueError("Valid job context types: cli, airflow, docker-itest, env - "
                         "consider upgrading records-mover if you're looking for "
                         f"{session_type}.")


class PleaseInfer(Enum):
    # This is a mypy-friendly way of doing a singleton object:
    #
    # https://github.com/python/typing/issues/236
    token = 1


class Session():
    def __init__(self,
                 default_db_creds_name: Optional[str] = None,
                 default_aws_creds_name: Union[None, str, PleaseInfer] = PleaseInfer.token,
                 session_type: Union[str, PleaseInfer] = PleaseInfer.token,
                 scratch_s3_url: Union[None, str, PleaseInfer] = PleaseInfer.token,
                 creds: Union[BaseCreds, PleaseInfer] = PleaseInfer.token) -> None:
        if session_type is PleaseInfer.token:
            session_type = _infer_session_type()

        if creds is PleaseInfer.token:
            creds = _infer_creds(session_type)

        if scratch_s3_url is PleaseInfer.token:
            scratch_s3_url = _infer_scratch_s3_url(session_type)

        if default_aws_creds_name is PleaseInfer.token:
            default_aws_creds_name = _infer_default_aws_creds_name(session_type)

        self._default_db_creds_name = default_db_creds_name
        self._default_aws_creds_name = default_aws_creds_name
        self._scratch_s3_url = scratch_s3_url
        self.creds = creds
        url_resolver_kwargs = {}
        boto3_session = self._boto3_session()
        if boto3_session:
            url_resolver_kwargs['boto3_session'] = boto3_session
        self.url_resolver = UrlResolver(**url_resolver_kwargs)

    def get_default_db_engine(self) -> 'Engine':
        from .db.connect import engine_from_db_facts

        if self._default_db_creds_name is None:
            db_facts = db_facts_from_env()
            return engine_from_db_facts(db_facts)
        else:
            return self.get_db_engine(self._default_db_creds_name)

    def get_default_db_facts(self) -> DBFacts:
        if self._default_db_creds_name is None:
            return db_facts_from_env()
        else:
            return self.creds.db_facts(self._default_db_creds_name)

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
        if self._scratch_s3_url is not None:
            try:
                s3_temp_base_loc = self.directory_url(self._scratch_s3_url)
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

    def _boto3_session(self) -> Optional['boto3.session.Session']:
        try:
            import boto3  # noqa
        except ModuleNotFoundError:
            logger.debug("boto3 not installed",
                         exc_info=True)
            return None

        if self._default_aws_creds_name is None:
            return boto3.session.Session()
        else:
            return self.creds.boto3_session(self._default_aws_creds_name)

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
