import boto3
from urllib.parse import urlparse
from airflow.exceptions import AirflowException
from records_mover.records.records import Records
from records_mover.db.factory import db_driver
from records_mover.db import DBDriver
from records_mover.url.resolver import UrlResolver
from airflow.hooks import BaseHook
from airflow.contrib.hooks.aws_hook import AwsHook
from typing import Optional, Union, List, TYPE_CHECKING
import sqlalchemy
if TYPE_CHECKING:
    from boto3.session import ListObjectsResponseContentType, S3ClientTypeStub  # noqa


class RecordsHook(BaseHook):
    "Airflow Hook which provides Records object"

    def __init__(self,
                 s3_temp_base_url: Optional[str] = None,
                 aws_conn_id: str = 'aws_default'):
        """
        Create a new RecordsHook

        :param s3_temp_base_url: If provided, use this URL for any temporary storage which requires
          use of S3.  If not provided, any operations (e.g., Redshift load/unloads) which are not
          directly provided an S3 bucket will raise.
        :param aws_conn_id: Airflow connection ID to use for AWS credentials.
        """
        # # noqa: E501
        self.aws_conn_id = aws_conn_id
        self.__s3_temp_base_url = s3_temp_base_url
        self._boto3_session: Optional[boto3.session.Session] = None

    def _get_boto3_session(self) -> boto3.session.Session:
        if not self._boto3_session:
            self._boto3_session = AwsHook(self.aws_conn_id).get_session()
        return self._boto3_session

    @property
    def _url_resolver(self) -> UrlResolver:
        return UrlResolver(boto3_session_getter=self._get_boto3_session,
                           gcs_client_getter=lambda: None,
                           gcp_credentials_getter=lambda: None)

    def _db_driver(self, db: Union[sqlalchemy.engine.Engine,
                                   sqlalchemy.engine.Connection]) -> DBDriver:
        s3_temp_base_loc = (self._url_resolver.directory_url(self._s3_temp_base_url)
                            if self._s3_temp_base_url else None)

        return db_driver(db=db, url_resolver=self._url_resolver, s3_temp_base_loc=s3_temp_base_loc)

    @property
    def _s3_temp_base_url(self) -> Optional[str]:
        if self.__s3_temp_base_url and not self.__s3_temp_base_url.endswith('/'):
            raise ValueError("Please provide a directory name - "
                             f"URL should end with '/': {self.__s3_temp_base_url}")
        return self.__s3_temp_base_url

    # Docstring has been left out below - this is not part of the
    # public API and should be evaluated for use in the records-mover
    # non-Airflow-specific API.
    def validate_and_prepare_target_directory(self,
                                              target_url: str,
                                              allow_overwrite: bool=False) -> None:
        parsed_target = urlparse(target_url)
        if parsed_target.scheme != 's3':
            raise AirflowException(
                f"unsupported scheme '{parsed_target.scheme}': {target_url}")
        target_bucket = parsed_target.netloc
        target_key = (parsed_target.path + '/').strip('/')

        s3: 'S3ClientTypeStub' = self._get_boto3_session().client('s3')
        objects: List['ListObjectsResponseContentType'] = s3.list_objects_v2(
            Bucket=target_bucket,
            Prefix=target_key,
        ).get('Contents', [])
        if len(objects) > 0:
            self.log.info("Target URL %s is non-empty (%d keys)", target_url, len(objects))
            if allow_overwrite:
                self.log.warning("Deleting contents of target %s", target_url)
                s3.delete_objects(
                    Bucket=target_bucket,
                    Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]}
                )
            else:
                raise AirflowException('Target URL %s is non-empty', target_url)

    def get_conn(self) -> Records:
        """
        :return: Records object which can be used for moving records data
        :rtype: records_mover.records.Records
        """
        return Records(
            db_driver=self._db_driver,
            url_resolver=self._url_resolver,
        )
