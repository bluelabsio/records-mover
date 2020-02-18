from .base_job_context import BaseJobContext
from .creds.creds_via_env import CredsViaEnv
from .creds.base_creds import BaseCreds


class EnvJobContext(BaseJobContext):
    """
    Job context which obtains secrets from env variables.
    """
    @property
    def creds(self) -> BaseCreds:
        return CredsViaEnv()
