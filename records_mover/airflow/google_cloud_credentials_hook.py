from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from typing import Iterable, Optional, TYPE_CHECKING
if TYPE_CHECKING:
    # see the 'gsheets' extras_require option in setup.py - needed for this!
    import google.auth.credentials  # noqa


class GoogleCloudCredentialsHook(GoogleCloudBaseHook):
    def get_conn(self) -> 'google.auth.credentials.Credentials':
        return self._get_credentials()

    def scopes(self) -> Iterable[str]:
        scope: Optional[str] = self._get_field('scope', None)
        scopes: Iterable[str]
        if scope is not None:
            scopes = [s.strip() for s in scope.split(',')]
        else:
            # https://github.com/apache/incubator-airflow/blob/master/airflow/contrib/hooks/gcp_api_base_hook.py#L32
            scopes = ('https://www.googleapis.com/auth/cloud-platform',)
        return scopes
