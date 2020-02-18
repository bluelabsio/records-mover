from .base_job_context import BaseJobContext
from .creds.creds_via_lastpass import CredsViaLastPass
from .creds.base_creds import BaseCreds


class IntegrationTestJobContext(BaseJobContext):
    """
    Bare bones job context for use during integration testing that
    doesn't assume the BlueLabs macOS desktop environment.
    """
    @property
    def creds(self) -> BaseCreds:
        return CredsViaLastPass()
