from enum import Enum
from records_mover.creds.creds_via_lastpass import CredsViaLastPass
from records_mover.creds.creds_via_airflow import CredsViaAirflow
from records_mover.creds.creds_via_env import CredsViaEnv
from records_mover.creds.base_creds import BaseCreds
from .base_job_context import BaseJobContext
from typing import Optional, Union
import subprocess
import os
import logging

logger = logging.getLogger(__name__)


def _infer_job_context_type() -> str:
    if 'PY_JOB_CONTEXT' in os.environ:
        return os.environ['PY_JOB_CONTEXT']

    if 'AIRFLOW__CORE__EXECUTOR' in os.environ:
        # Guess based on an env variable sometimes set by Airflow
        return 'airflow'

    return 'cli'


def _infer_scratch_s3_url(job_context_type: str) -> Optional[str]:
    scratch_s3_url: Optional[str]

    if "SCRATCH_S3_URL" in os.environ:
        return os.environ["SCRATCH_S3_URL"]

    if job_context_type == 'cli':
        try:
            return subprocess.check_output("scratch-s3-url").decode('ASCII').rstrip()
        except FileNotFoundError:
            pass
    return None


def _infer_default_aws_creds_name(job_context_type: str) -> Optional[str]:
    if job_context_type == 'airflow':
        return 'aws_default'
    return None


def _infer_creds(job_context_type: str) -> BaseCreds:
    if job_context_type == 'airflow':
        return CredsViaAirflow()
    elif job_context_type == 'cli':
        return CredsViaLastPass()
    elif job_context_type == 'itest':
        return CredsViaLastPass()
    elif job_context_type == 'env':
        return CredsViaEnv()
    elif job_context_type is not None:
        raise ValueError("Valid job context types: cli, airflow, docker-itest, env - "
                         "consider upgrading records-mover if you're looking for "
                         f"{job_context_type}.")


class PleaseInfer(Enum):
    # This is a mypy-friendly way of doing a singleton object:
    #
    # https://github.com/python/typing/issues/236
    token = 1


def get_job_context(default_db_creds_name: Optional[str] = None,
                    default_aws_creds_name: Union[None, str, PleaseInfer] = PleaseInfer.token,
                    job_context_type: Union[str, PleaseInfer] = PleaseInfer.token,
                    scratch_s3_url: Union[None, str, PleaseInfer] = PleaseInfer.token,
                    creds: Union[BaseCreds, PleaseInfer] = PleaseInfer.token) -> BaseJobContext:
    if job_context_type is PleaseInfer.token:
        job_context_type = _infer_job_context_type()

    if creds is PleaseInfer.token:
        creds = _infer_creds(job_context_type)

    if scratch_s3_url is PleaseInfer.token:
        scratch_s3_url = _infer_scratch_s3_url(job_context_type)

    if default_aws_creds_name is PleaseInfer.token:
        default_aws_creds_name = _infer_default_aws_creds_name(job_context_type)

    return BaseJobContext(creds=creds,
                          default_db_creds_name=default_db_creds_name,
                          default_aws_creds_name=default_aws_creds_name,
                          scratch_s3_url=scratch_s3_url)
