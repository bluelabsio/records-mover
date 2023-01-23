import boto3
from typing import Optional
from airflow.hooks.base import BaseHook


# https://github.com/apache/airflow/blob/master/airflow/contrib/hooks/aws_hook.py
class AwsBaseHook(BaseHook):
    def __init__(self, conn_id: str):
        ...

    def get_session(self, region_name: Optional[str] = None) -> boto3.session.Session:
        ...
