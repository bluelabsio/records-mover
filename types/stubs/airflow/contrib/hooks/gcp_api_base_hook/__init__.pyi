from typing import Any, TYPE_CHECKING
if TYPE_CHECKING:
    # see the 'gsheets' extras_require option in setup.py - needed for this!
    import google.auth.credentials  # noqa


class GoogleBaseHook:
    def __init__(self, gcp_conn_id: str):
        ...

    def _get_credentials(self) -> 'google.auth.credentials.Credentials':
        ...

    def _get_field(self, field: str, default: Any) -> Any:
        ...
