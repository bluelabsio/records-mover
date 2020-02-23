import sys
import subprocess
from ..base_job_context import BaseJobContext, JsonSchema
from .job_config_schema_as_args_parser import JobConfigSchemaAsArgsParser
from typing import Optional, Sequence
from ..creds.base_creds import BaseCreds
from ..creds.creds_via_lastpass import CredsViaLastPass


class CLIJobContext(BaseJobContext):
    __creds_via_lastpass_value: Optional[CredsViaLastPass]

    def __init__(self,
                 name: str,
                 creds: BaseCreds,
                 default_db_creds_name: Optional[str],
                 default_aws_creds_name: Optional[str],
                 config_json_schema: Optional[JsonSchema],
                 scratch_s3_url: Optional[str] = None,
                 args: Sequence[str]=sys.argv[1:]) -> None:
        super().__init__(name=name,
                         creds=creds,
                         default_db_creds_name=default_db_creds_name,
                         default_aws_creds_name=default_aws_creds_name,
                         config_json_schema=config_json_schema,
                         scratch_s3_url=scratch_s3_url)
        if self._scratch_s3_url is None:
            # check_output() is typed as returning Any in typeshed
            # - depends on encoding argument...
            try:
                self._scratch_s3_url: Optional[str] =\
                    subprocess.check_output("scratch-s3-url").decode('ASCII').rstrip()
            except FileNotFoundError:
                self._scratch_s3_url = None
        if self._config_json_schema is not None:
            self.request_config =\
                JobConfigSchemaAsArgsParser.from_description(self._config_json_schema,
                                                             name).parse(args)
        else:
            self.request_config = {}
