from .cli.cli_job_context import CLIJobContext
from .itest_job_context import IntegrationTestJobContext
from .env_job_context import EnvJobContext
from .airflow.airflow_job_context import AirflowJobContext
from .base_job_context import BaseJobContext, JsonSchema
from typing import Optional, Type
import os


def get_job_context(default_db_creds_name: Optional[str]=None,
                    default_aws_creds_name: Optional[str]=None,
                    scratch_s3_url: Optional[str]=None,
                    config_json_schema: Optional[JsonSchema]=None,
                    job_context_type: Optional[str]=None) -> BaseJobContext:
    if job_context_type is None:
        job_context_type = os.environ.get('PY_JOB_CONTEXT')
    jc_class: Type[BaseJobContext]
    if job_context_type == 'airflow':
        jc_class = AirflowJobContext
    elif job_context_type == 'cli':
        jc_class = CLIJobContext
    elif job_context_type == 'itest':
        jc_class = IntegrationTestJobContext
    elif job_context_type == 'env':
        jc_class = EnvJobContext
    elif job_context_type is not None:
        raise ValueError("Valid job context types: cli, airflow, docker-itest, env - "
                         "consider upgrading records-mover if you're looking for "
                         f"{job_context_type}.")
    elif 'AIRFLOW__CORE__EXECUTOR' in os.environ:
        # Guess based on an env variable sometimes set by Airflow
        jc_class = AirflowJobContext
    else:
        jc_class = CLIJobContext

    return jc_class(config_json_schema=config_json_schema,
                    default_db_creds_name=default_db_creds_name,
                    default_aws_creds_name=default_aws_creds_name,
                    scratch_s3_url=scratch_s3_url)
