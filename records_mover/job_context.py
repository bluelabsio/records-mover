from records_mover.creds.creds_via_lastpass import CredsViaLastPass
from records_mover.creds.creds_via_airflow import CredsViaAirflow
from records_mover.creds.creds_via_env import CredsViaEnv
from records_mover.creds.base_creds import BaseCreds
from .cli.cli_job_context import CLIJobContext
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
    creds: BaseCreds

    if scratch_s3_url is None:
        if "SCRATCH_S3_URL" in os.environ:
            scratch_s3_url = os.environ["SCRATCH_S3_URL"]

    if job_context_type == 'airflow':
        jc_class = BaseJobContext
        creds = CredsViaAirflow()
        if default_aws_creds_name is None:
            default_aws_creds_name = 'aws_default'
    elif job_context_type == 'cli':
        jc_class = CLIJobContext
        creds = CredsViaLastPass()
    elif job_context_type == 'itest':
        jc_class = BaseJobContext
        creds = CredsViaLastPass()
    elif job_context_type == 'env':
        jc_class = BaseJobContext
        creds = CredsViaEnv()
    elif job_context_type is not None:
        raise ValueError("Valid job context types: cli, airflow, docker-itest, env - "
                         "consider upgrading records-mover if you're looking for "
                         f"{job_context_type}.")
    elif 'AIRFLOW__CORE__EXECUTOR' in os.environ:
        # Guess based on an env variable sometimes set by Airflow
        jc_class = BaseJobContext
        creds = CredsViaAirflow()
        if default_aws_creds_name is None:
            default_aws_creds_name = 'aws_default'
    else:
        jc_class = CLIJobContext
        creds = CredsViaLastPass()

    return jc_class(creds=creds,
                    config_json_schema=config_json_schema,
                    default_db_creds_name=default_db_creds_name,
                    default_aws_creds_name=default_aws_creds_name,
                    scratch_s3_url=scratch_s3_url)
