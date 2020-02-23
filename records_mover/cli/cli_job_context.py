import subprocess
from ..base_job_context import BaseJobContext
from typing import Optional
from ..creds.base_creds import BaseCreds


class CLIJobContext(BaseJobContext):
    def __init__(self,
                 creds: BaseCreds,
                 default_db_creds_name: Optional[str],
                 default_aws_creds_name: Optional[str],
                 scratch_s3_url: Optional[str] = None) -> None:
        super().__init__(creds=creds, default_db_creds_name=default_db_creds_name,
                         default_aws_creds_name=default_aws_creds_name,
                         scratch_s3_url=scratch_s3_url)
        if self._scratch_s3_url is None:
            # check_output() is typed as returning Any in typeshed
            # - depends on encoding argument...
            try:
                self._scratch_s3_url: Optional[str] =\
                    subprocess.check_output("scratch-s3-url").decode('ASCII').rstrip()
            except FileNotFoundError:
                self._scratch_s3_url = None
