from contextlib import contextmanager

from .cli import cli_logging
from . import log_levels
from .cli.cli_job_context import CLIJobContext
from .itest_job_context import IntegrationTestJobContext
from .env_job_context import EnvJobContext
from .airflow.airflow_job_context import AirflowJobContext
from .temp_dir import set_temp_dir
from .base_job_context import BaseJobContext, JsonSchema
import atexit
from typing import Iterator, Optional, Type
import os


@contextmanager
def create_job_context(name: str, **kwargs) -> Iterator[BaseJobContext]:
    """Use this to run a job.  It will yield a job context, which you can use
    to get information about your job's place in the world!
    """
    jc = __get_job_context(name, **kwargs)
    try:
        with set_temp_dir(jc.temp_dir):
            yield jc
    finally:
        jc.cleanup()


def pull_job_context(name: str, **kwargs) -> BaseJobContext:
    """Returns a job context.  Cleans up temporary resources at process
    exit time, rather than as a context manager - see
    create_job_context for that.
    """
    job_context_contextmanager = create_job_context(name, **kwargs)
    atexit.register(lambda: job_context_contextmanager.__exit__(None, None, None))
    return job_context_contextmanager.__enter__()


def run_as_job(fn, name: str, **kwargs) -> None:
    with create_job_context(name, **kwargs) as job_context:
        fn(job_context)


def __get_job_context(name: str,
                      use_default_logging: bool=True,
                      default_db_creds_name: Optional[str]=None,
                      default_aws_creds_name: Optional[str]=None,
                      scratch_s3_url: Optional[str]=None,
                      config_json_schema: JsonSchema=None,
                      job_context_type: Optional[str]=None) -> BaseJobContext:
    if job_context_type is None:
        job_context_type = os.environ.get('PY_JOB_CONTEXT')
    log_levels.set_levels(name)
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

    if jc_class != AirflowJobContext:
        if use_default_logging:
            cli_logging.basic_config()

    return jc_class(name,
                    config_json_schema=config_json_schema,
                    default_db_creds_name=default_db_creds_name,
                    default_aws_creds_name=default_aws_creds_name,
                    scratch_s3_url=scratch_s3_url)
