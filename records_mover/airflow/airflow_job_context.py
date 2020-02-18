from ..creds.base_creds import BaseCreds
from ..creds.creds_via_airflow import CredsViaAirflow
from ..base_job_context import BaseJobContext, JsonSchema
from typing import Optional


class AirflowJobContext(BaseJobContext):
    __creds_via_airflow_value: Optional[CredsViaAirflow] = None

    def __init__(self,
                 name: str,
                 default_db_creds_name: Optional[str],
                 default_aws_creds_name: Optional[str],
                 config_json_schema: Optional[JsonSchema],
                 scratch_s3_url: Optional[str] = None) -> None:
        if default_aws_creds_name is None:
            default_aws_creds_name = 'aws_default'
        super().__init__(name=name,
                         default_db_creds_name=default_db_creds_name,
                         default_aws_creds_name=default_aws_creds_name,
                         scratch_s3_url=scratch_s3_url,
                         config_json_schema=config_json_schema)
        self.__request_config = {}

    @property
    def creds(self) -> BaseCreds:
        return self.creds_via_airflow

    @property
    def creds_via_airflow(self) -> CredsViaAirflow:
        if self.__creds_via_airflow_value is None:
            self.__creds_via_airflow_value = CredsViaAirflow()
        return self.__creds_via_airflow_value
