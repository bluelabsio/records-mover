from .database import db_engine, db_facts_from_env
from sqlalchemy.engine import Engine
import boto3
from .records.records import Records
from .db import db_driver, DBDriver
from .url.base import BaseFileUrl, BaseDirectoryUrl
import sqlalchemy
from typing import Union, Optional
from .creds.base_creds import BaseCreds
from .db.connect import engine_from_db_facts
from .url.resolver import UrlResolver
from db_facts.db_facts_types import DBFacts
from enum import Enum
from records_mover.creds.creds_via_lastpass import CredsViaLastPass
from records_mover.creds.creds_via_airflow import CredsViaAirflow
from records_mover.creds.creds_via_env import CredsViaEnv
import subprocess
import os
import logging


logger = logging.getLogger(__name__)


def _infer_session_type() -> str:
    if 'RECORDS_MOVER_SESSION_TYPE' in os.environ:
        return os.environ['RECORDS_MOVER_SESSION_TYPE']

    if 'AIRFLOW__CORE__EXECUTOR' in os.environ:
        # Guess based on an env variable sometimes set by Airflow
        return 'airflow'

    return 'cli'


def _infer_scratch_s3_url(session_type: str) -> Optional[str]:
    scratch_s3_url: Optional[str]

    if "SCRATCH_S3_URL" in os.environ:
        return os.environ["SCRATCH_S3_URL"]

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
        self.url_resolver = UrlResolver(boto3_session=self._boto3_session())

    def get_default_db_engine(self) -> Engine:
        if self._default_db_creds_name is None:
            return db_engine(self)
        else:
            return self.get_db_engine(self._default_db_creds_name)

    def get_default_db_facts(self) -> DBFacts:
        if self._default_db_creds_name is None:
            return db_facts_from_env()
        else:
            return self.creds.db_facts(self._default_db_creds_name)

    def get_db_engine(self,
                      db_creds_name: str,
                      creds_provider: Optional[BaseCreds] = None) -> Engine:
        if creds_provider is None:
            creds_provider = self.creds
        db_facts = creds_provider.db_facts(db_creds_name)
        return engine_from_db_facts(db_facts)

    def db_driver(self, db: Union[sqlalchemy.engine.Engine,
                                  sqlalchemy.engine.Connection]) -> DBDriver:
        s3_temp_base_loc = None
        if self._scratch_s3_url is not None:
            s3_temp_base_loc = self.directory_url(self._scratch_s3_url)
        return db_driver(db=db,
                         s3_temp_base_loc=s3_temp_base_loc,
                         url_resolver=self.url_resolver)

    def file_url(self, url: str) -> BaseFileUrl:
        return self.url_resolver.file_url(url)

    def directory_url(self, url: str) -> BaseDirectoryUrl:
        return self.url_resolver.directory_url(url)

    def _boto3_session(self) -> boto3.session.Session:
        if self._default_aws_creds_name is None:
            return boto3.session.Session()
        else:
            return self.creds.boto3_session(self._default_aws_creds_name)

    @property
    def records(self) -> Records:
        return Records(db_driver=self.db_driver,
                       url_resolver=self.url_resolver)
