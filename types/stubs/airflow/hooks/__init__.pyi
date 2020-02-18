from logging import Logger
from airflow.models import Connection


class BaseHook:
    log: Logger

    @classmethod
    def get_connection(cls, creds_name: str) -> Connection:
        ...
