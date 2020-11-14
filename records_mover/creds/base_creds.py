from db_facts.db_facts_types import DBFacts
from .database import db_facts_from_env
from typing import TYPE_CHECKING, Iterable, Union, Optional, Dict, Any
from records_mover.mover_types import PleaseInfer
from config_resolver import get_config
import os
import logging
if TYPE_CHECKING:
    # see the 'gsheets' extras_require option in setup.py - needed for this!
    import google.auth.credentials  # noqa
    import boto3  # noqa


logger = logging.getLogger(__name__)

_GSHEETS_SCOPES = ('https://www.googleapis.com/auth/spreadsheets',)

_GCS_SCOPES = ('https://www.googleapis.com/auth/devstorage.full_control',
               'https://www.googleapis.com/auth/devstorage.read_only',
               'https://www.googleapis.com/auth/devstorage.read_write')


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
# situation, let the session pick which backend we're using for
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
                                       None] = PleaseInfer.token,
                 scratch_gcs_url: Union[PleaseInfer,
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
        self._scratch_gcs_url = scratch_gcs_url

    def google_sheets(self, gcp_creds_name: str) -> 'google.auth.credentials.Credentials':
        scopes = _GSHEETS_SCOPES
        return self._gcp_creds(gcp_creds_name, scopes)

    def gcs(self, gcp_creds_name: str) -> 'google.auth.credentials.Credentials':
        scopes = _GCS_SCOPES
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

    def _gcp_creds_of_last_resort(self,
                                  scopes: Iterable[str]) ->\
            Optional['google.auth.credentials.Credentials']:
        import google.auth
        credentials, project = google.auth.default(scopes=scopes)
        return credentials

    def default_gcs_creds(self) -> Optional['google.auth.credentials.Credentials']:
        if self.__default_gcs_creds is not PleaseInfer.token:
            return self.__default_gcs_creds

        try:
            import google.auth.exceptions
            if self._default_gcp_creds_name is None:
                self.__default_gcs_creds = self._gcp_creds_of_last_resort(scopes=_GCS_SCOPES)
            else:
                self.__default_gcs_creds = self.gcs(self._default_gcp_creds_name)
        except (OSError, google.auth.exceptions.DefaultCredentialsError) as e:
            # Examples:
            #   OSError: Project was not passed and could not be determined from the environment.
            #   google.auth.exceptions.DefaultCredentialsError: Could not automatically determine
            #     credentials. Please set GOOGLE_APPLICATION_CREDENTIALS or explicitly create
            #     credentials and re-run the application. For more information, please see
            #     https://cloud.google.com/docs/authentication/getting-started
            logger.warning(f"google.cloud.storage not configured: {e}")
            logger.debug("Details:", exc_info=True)
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
        except ModuleNotFoundError as e:
            logger.warning(f"google.cloud.storage not installed: {e}")
            logger.debug("Details:", exc_info=True)
            self.__default_gcs_client = None
            return self.__default_gcs_client

        try:
            other_args = {}
            # Pulling the project here from global config instead of
            # from the Google API and letting the user configure it
            # isn't great.  See
            # https://github.com/bluelabsio/records-mover/issues/119
            # for a way we can improve this.
            gcp_project = self._default_gcp_project()
            if gcp_project is not None:
                other_args['project'] = gcp_project

            self.__default_gcs_client = google.cloud.storage.Client(credentials=gcs_creds,
                                                                    **other_args)
            return self.__default_gcs_client
        except OSError as e:
            # Example:
            #   OSError: Project was not passed and could not be determined from the environment.
            logger.warning(f"google.cloud.storage not configured: {e}")
            logger.debug("Details:", exc_info=True)
            self.__default_gcs_client = None
            return self.__default_gcs_client

    def _default_gcp_project(self) -> Optional[str]:
        if 'GCP_PROJECT' in os.environ:
            return os.environ['GCP_PROJECT']
        gcp_cfg = self._config_section('gcp')
        if gcp_cfg is not None:
            default_gcp_project: Optional[str] = gcp_cfg.get('default_project')
            return default_gcp_project

        return None

    def default_db_facts(self) -> DBFacts:
        if self.__default_db_facts is not PleaseInfer.token:
            return self.__default_db_facts

        if self._default_db_creds_name is None:
            self.__default_db_facts = db_facts_from_env()
        else:
            self.__default_db_facts = self.db_facts(self._default_db_creds_name)
        return self.__default_db_facts

    def _append_aws_username_to_bucket(self,
                                       prefix: str,
                                       boto3_session: 'boto3.session.Session') -> Optional[str]:
        sts_client = boto3_session.client('sts')
        caller_identity = sts_client.get_caller_identity()
        arn = caller_identity['Arn']
        last_section_of_arn = arn.split(':')[-1]
        # Check that this is an actual user and not, say, an assumed
        # role or something else.
        if last_section_of_arn.startswith('user/'):
            username = last_section_of_arn.split('/')[-1]
            return f"{prefix}{username}/"
        else:
            logger.warning('Cannot generate S3 scratch URL with IAM username, '
                           f'as there is no username in {arn}')
            return None

    def _config_section(self, section_name: str) -> Optional[Dict[str, Any]]:
        config_result = get_config('records_mover', 'bluelabs')
        cfg = config_result.config
        if section_name in cfg:
            return cfg[section_name]
        else:
            return None

    def _infer_scratch_s3_url(self,
                              boto3_session: Optional['boto3.session.Session']) -> Optional[str]:
        if "SCRATCH_S3_URL" in os.environ:
            return os.environ["SCRATCH_S3_URL"]

        aws_cfg = self._config_section('aws')
        if aws_cfg is not None:
            s3_scratch_url: Optional[str] = aws_cfg.get('s3_scratch_url')
            if s3_scratch_url is not None:
                return s3_scratch_url
            else:
                s3_scratch_url_prefix: Optional[str] =\
                    aws_cfg.get('s3_scratch_url_appended_with_iam_username')
                if s3_scratch_url_prefix is not None:
                    if boto3_session is None:
                        logger.warning('Cannot generate S3 scratch URL with IAM username, '
                                       'as I have no IAM username')
                        return None
                    return self._append_aws_username_to_bucket(s3_scratch_url_prefix,
                                                               boto3_session)
                else:
                    logger.debug('No S3 scratch bucket config found')
                    return None
        else:
            logger.debug('No config ini file found')
            return None

    def default_scratch_s3_url(self) -> Optional[str]:
        if self._scratch_s3_url is PleaseInfer.token:
            self._scratch_s3_url = self._infer_scratch_s3_url(self.default_boto3_session())
        return self._scratch_s3_url

    def _infer_scratch_gcs_url(self) -> Optional[str]:
        if "SCRATCH_GCS_URL" in os.environ:
            return os.environ["SCRATCH_GCS_URL"]

        gcp_cfg = self._config_section('gcp')
        if gcp_cfg is not None:
            gcs_scratch_url: Optional[str] = gcp_cfg.get('gcs_scratch_url')
            if gcs_scratch_url is not None:
                return gcs_scratch_url
            else:
                logger.debug('No GCS scratch bucket config found')
                return None
        else:
            logger.debug('No config ini file found')
            return None

    def default_scratch_gcs_url(self) -> Optional[str]:
        if self._scratch_gcs_url is PleaseInfer.token:
            self._scratch_gcs_url = self._infer_scratch_gcs_url()
        return self._scratch_gcs_url
