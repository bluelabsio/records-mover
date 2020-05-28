import logging
import boto3
from .s3_file_url import S3FileUrl
from .s3_directory_url import S3DirectoryUrl
from typing import Union

logger = logging.getLogger(__name__)


def S3Url(url: str, boto3_session: boto3.session.Session, **kwargs: object) -> Union[S3DirectoryUrl,
                                                                                     S3FileUrl]:
    if url.endswith('/'):
        return S3DirectoryUrl(url, boto3_session=boto3_session, S3Url=S3Url)
    else:
        return S3FileUrl(url, boto3_session=boto3_session, S3Url=S3Url)
