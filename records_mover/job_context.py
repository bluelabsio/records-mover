from records_mover.creds.creds_via_lastpass import CredsViaLastPass
from records_mover.creds.creds_via_airflow import CredsViaAirflow
from records_mover.creds.creds_via_env import CredsViaEnv
from records_mover.creds.base_creds import BaseCreds
from .base_job_context import BaseJobContext
from typing import Optional
import subprocess
import os
import logging

logger = logging.getLogger(__name__)


def get_job_context(default_db_creds_name: Optional[str]=None,
                    default_aws_creds_name: Optional[str]=None,
                    scratch_s3_url: Optional[str]=None,
                    job_context_type: Optional[str]=None) -> BaseJobContext:
    if job_context_type is None:
        job_context_type = os.environ.get('PY_JOB_CONTEXT')
    creds: BaseCreds

    if scratch_s3_url is None:
        if "SCRATCH_S3_URL" in os.environ:
            scratch_s3_url = os.environ["SCRATCH_S3_URL"]

    if job_context_type is None:
        if 'AIRFLOW__CORE__EXECUTOR' in os.environ:
            # Guess based on an env variable sometimes set by Airflow
            job_context_type = 'airflow'
        else:
            job_context_type = 'cli'

    if job_context_type == 'airflow':
        creds = CredsViaAirflow()
        if default_aws_creds_name is None:
            default_aws_creds_name = 'aws_default'
    elif job_context_type == 'cli':
        creds = CredsViaLastPass()
        if scratch_s3_url is None:
            try:
                scratch_s3_url =\
                    subprocess.check_output("scratch-s3-url").decode('ASCII').rstrip()
            except FileNotFoundError:
                logger.debug
                pass
    elif job_context_type == 'itest':
        creds = CredsViaLastPass()
    elif job_context_type == 'env':
        creds = CredsViaEnv()
    elif job_context_type is not None:
        raise ValueError("Valid job context types: cli, airflow, docker-itest, env - "
                         "consider upgrading records-mover if you're looking for "
                         f"{job_context_type}.")

    return BaseJobContext(creds=creds,
                          default_db_creds_name=default_db_creds_name,
                          default_aws_creds_name=default_aws_creds_name,
                          scratch_s3_url=scratch_s3_url)
