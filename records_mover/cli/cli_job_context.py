import subprocess
from ..base_job_context import BaseJobContext
from typing import Optional
from ..creds.base_creds import BaseCreds
from ..creds.creds_via_lastpass import CredsViaLastPass


class CLIJobContext(BaseJobContext):
    __creds_via_lastpass_value: Optional[CredsViaLastPass]

    def __init__(self,
                 default_db_creds_name: Optional[str],
                 default_aws_creds_name: Optional[str],
                 scratch_s3_url: Optional[str] = None) -> None:
        super().__init__(default_db_creds_name=default_db_creds_name,
                         default_aws_creds_name=default_aws_creds_name,
                         scratch_s3_url=scratch_s3_url)
        self.__creds_via_lastpass_value = None

    @property
    def creds(self) -> BaseCreds:
        return self.creds_via_lastpass

    @property
    def creds_via_lastpass(self) -> CredsViaLastPass:
        if self.__creds_via_lastpass_value is None:
            self.__creds_via_lastpass_value = CredsViaLastPass()
        return self.__creds_via_lastpass_value

    @property
    def _scratch_s3_url(self) -> Optional[str]:
        if super()._scratch_s3_url is None:
            # check_output() is typed as returning Any in typeshed
            # - depends on encoding argument...
            try:
                self._scratch_s3_url_value: Optional[str] =\
                    subprocess.check_output("scratch-s3-url").decode('ASCII').rstrip()
            except FileNotFoundError:
                self._scratch_s3_url_value = None

        return self._scratch_s3_url_value
