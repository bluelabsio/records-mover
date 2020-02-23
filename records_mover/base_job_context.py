from abc import abstractproperty, ABCMeta
import os
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


class BaseJobContext(metaclass=ABCMeta):
    _scratch_s3_url_value: Optional[str]

    def __init__(self,
                 default_db_creds_name: Optional[str],
                 default_aws_creds_name: Optional[str],
                 scratch_s3_url: Optional[str] = None) -> None:
        self._default_db_creds_name = default_db_creds_name
        self._default_aws_creds_name = default_aws_creds_name
        self._scratch_s3_url_value = scratch_s3_url
        self.url_resolver = UrlResolver(boto3_session=self._boto3_session())

    @abstractproperty
    def creds(self) -> BaseCreds:
        raise NotImplementedError

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

    @property
    def _scratch_s3_url(self) -> Optional[str]:
        if self._scratch_s3_url_value is None:
            if "SCRATCH_S3_URL" in os.environ:
                self._scratch_s3_url_value = os.environ["SCRATCH_S3_URL"]
        if self._scratch_s3_url_value is not None:
            if not self._scratch_s3_url_value.endswith('/'):
                raise ValueError("Please provide a directory name - "
                                 f"URL should end with '/': {self._scratch_s3_url_value}")
        return self._scratch_s3_url_value
