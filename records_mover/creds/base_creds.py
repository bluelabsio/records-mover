from db_facts.db_facts_types import DBFacts
from .database import db_facts_from_env
from typing import TYPE_CHECKING, Iterable, Union, Optional
from records_mover.mover_types import PleaseInfer
from config_resolver import get_config
import os
import logging
if TYPE_CHECKING:
    # see the 'gsheets' extras_require option in setup.py - needed for this!
    import google.auth.credentials  # noqa
    import boto3  # noqa


logger = logging.getLogger(__name__)


# this interfaces here are probably unstable until we figure out how
# best to integrate airflow hooks and connections (and maybe
# Kubernetes secrets) in with it.  The current (unfinalized and
# untested) thought is to use the *_creds_name argument to reflect the
# Airflow connection name, so a generic job can call
# session.creds.cred_type(some_name_from_its_arguments) and get a
# valid cred both on the command line and in Airflow.
#
# If this seems to work, we can extend this idea out to Kubernetes
# secrets by writing a separate backend for it (and if we need to
# support both Airflow and Kubernetes secrets depending on the
# situation, let the job context pick which backend we're using for
# creds based on out of band information, like how it was constructed
# or environment variables).
class BaseCreds():
    def __init__(self,
                 default_db_creds_name: Optional[str] = None,
                 default_aws_creds_name: Optional[str] = None,
                 default_gcp_creds_name: Optional[str] = None,
                 default_db_facts: Union[PleaseInfer, DBFacts] = PleaseInfer.token,
                 default_boto3_session: Union[PleaseInfer,
                                              'boto3.session.Session',
                                              None] = PleaseInfer.token,
                 default_gcp_creds: Union[PleaseInfer,
                                          'google.auth.credentials.Credentials',
                                          None] = PleaseInfer.token,
                 default_gcs_client: Union[PleaseInfer,
                                           'google.cloud.storage.Client',
                                           None] = PleaseInfer.token,
                 scratch_s3_url: Union[PleaseInfer,
                                       str,
                                       None] = PleaseInfer.token) -> None:
        self._default_db_creds_name = default_db_creds_name
        self._default_aws_creds_name = default_aws_creds_name
        self._default_gcp_creds_name = default_gcp_creds_name

        self.__default_db_facts = default_db_facts
        self.__default_gcs_creds = default_gcp_creds
        self.__default_gcs_client = default_gcs_client
        self.__default_boto3_session = default_boto3_session

        self._scratch_s3_url = scratch_s3_url

    def google_sheets(self, gcp_creds_name: str) -> 'google.auth.credentials.Credentials':
        scopes = ('https://www.googleapis.com/auth/spreadsheets',)
        return self._gcp_creds(gcp_creds_name, scopes)

    def gcs(self, gcp_creds_name: str) -> 'google.auth.credentials.Credentials':
        scopes = ('https://www.googleapis.com/auth/devstorage.full_control',
                  'https://www.googleapis.com/auth/devstorage.read_only',
                  'https://www.googleapis.com/auth/devstorage.read_write')
        return self._gcp_creds(gcp_creds_name, scopes)

    def _gcp_creds(self, gcp_creds_name: str,
                   scopes: Iterable[str]) -> 'google.auth.credentials.Credentials':
        raise NotImplementedError

    def db_facts(self, db_creds_name: str) -> DBFacts:
        raise NotImplementedError

    def boto3_session(self, aws_creds_name: str) -> 'boto3.session.Session':
        raise NotImplementedError

    def default_boto3_session(self) -> Optional['boto3.session.Session']:
        if self.__default_boto3_session is not PleaseInfer.token:
            return self.__default_boto3_session

        try:
            import boto3  # noqa
        except ModuleNotFoundError:
            logger.debug("boto3 not installed",
                         exc_info=True)
            return None

        if self._default_aws_creds_name is None:
            self.__default_boto3_session = boto3.session.Session()
        else:
            self.__default_boto3_session = self.boto3_session(self._default_aws_creds_name)
        return self.__default_boto3_session

    def default_gcs_creds(self) -> Optional['google.auth.credentials.Credentials']:
        if self.__default_gcs_creds is not PleaseInfer.token:
            return self.__default_gcs_creds

        try:
            import google.auth.exceptions
            if self._default_gcp_creds_name is None:
                import google.auth
                credentials, project = google.auth.default()
                self.__default_gcs_creds = credentials
            else:
                creds = self.gcs(self._default_gcp_creds_name)
                self.__default_gcs_creds = creds
        except (OSError, google.auth.exceptions.DefaultCredentialsError):
            # Examples:
            #   OSError: Project was not passed and could not be determined from the environment.
            #   google.auth.exceptions.DefaultCredentialsError: Could not automatically determine
            #     credentials. Please set GOOGLE_APPLICATION_CREDENTIALS or explicitly create
            #     credentials and re-run the application. For more information, please see
            #     https://cloud.google.com/docs/authentication/getting-started
            logger.debug("google.cloud.storage not configured",
                         exc_info=True)
            self.__default_gcs_creds = None
        return self.__default_gcs_creds

    def default_gcs_client(self) -> Optional['google.cloud.storage.Client']:
        if self.__default_gcs_client is not PleaseInfer.token:
            return self.__default_gcs_client

        gcs_creds = self.default_gcs_creds()
        if gcs_creds is None:
            self.__default_gcs_client = None
            return self.__default_gcs_client
        try:
            import google.cloud.storage  # noqa
        except ModuleNotFoundError:
            logger.debug("google.cloud.storage not installed",
                         exc_info=True)
            self.__default_gcs_client = None
            return self.__default_gcs_client

        try:
            self.__default_gcs_client = google.cloud.storage.Client(credentials=gcs_creds)
            return self.__default_gcs_client
        except OSError:
            # Example:
            #   OSError: Project was not passed and could not be determined from the environment.
            logger.debug("google.cloud.storage not configured",
                         exc_info=True)
            self.__default_gcs_client = None
            return self.__default_gcs_client

    def default_db_facts(self) -> DBFacts:
        if self.__default_db_facts is not PleaseInfer.token:
            return self.__default_db_facts

        if self._default_db_creds_name is None:
            self.__default_db_facts = db_facts_from_env()
        else:
            self.__default_db_facts = self.db_facts(self._default_db_creds_name)
        return self.__default_db_facts

    def _infer_scratch_s3_url(self,
                              boto3_session: Optional['boto3.session.Session']) -> Optional[str]:
        if "SCRATCH_S3_URL" in os.environ:
            return os.environ["SCRATCH_S3_URL"]

        config_result = get_config('records_mover', 'bluelabs')
        cfg = config_result.config
        if 'aws' in cfg:
            aws_cfg = cfg['aws']
            s3_scratch_url: Optional[str] = aws_cfg.get('s3_scratch_url')
            if s3_scratch_url is not None:
                append_iam_user_id_str: Optional[str] = aws_cfg.get('append_iam_user_id')
                if append_iam_user_id_str is not None:
                    if append_iam_user_id_str == 'true':
                        append_iam_user_id = True
                    elif append_iam_user_id_str == 'false':
                        append_iam_user_id = False
                    else:
                        raise SyntaxError("Could not understand append_iam_user value - "
                                          "Please set to true or false")
                    if append_iam_user_id:
                        # TODO: Make sure boto3_session is not None
                        sts_client = boto3_session.client('sts')
                        caller_identity = sts_client.get_caller_identity()
                        arn = caller_identity['Arn']
                        user_id = arn.split('/')[-1]
                        s3_scratch_url = f"{s3_scratch_url}/{user_id}/"
            return s3_scratch_url
        else:
            logger.debug('No config ini file found')

        return None

    def default_scratch_s3_url(self) -> Optional[str]:
        if self._scratch_s3_url is PleaseInfer.token:
            self._scratch_s3_url = self._infer_scratch_s3_url(self.default_boto3_session())
        return self._scratch_s3_url
