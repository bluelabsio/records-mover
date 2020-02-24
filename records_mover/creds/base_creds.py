import boto3
from db_facts.db_facts_types import DBFacts
from typing import TYPE_CHECKING, Iterable
if TYPE_CHECKING:
    # see the 'gsheets' extras_require option in setup.py - needed for this!
    import google.auth.credentials  # noqa


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
    def google_sheets(self, gcp_creds_name: str) -> 'google.auth.credentials.Credentials':
        scopes = ('https://www.googleapis.com/auth/spreadsheets',)
        return self._gcp_creds(gcp_creds_name, scopes)

    def _gcp_creds(self, gcp_creds_name: str,
                   scopes: Iterable[str]) -> 'google.auth.credentials.Credentials':
        raise NotImplementedError

    def db_facts(self, db_creds_name: str) -> DBFacts:
        raise NotImplementedError

    def boto3_session(self, aws_creds_name: str) -> boto3.session.Session:
        raise NotImplementedError
