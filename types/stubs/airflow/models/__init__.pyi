from typing import Any


class Connection:
    id: int
    conn_id: str
    conn_type: str
    host: str
    schema: str
    login: str
    password: str
    port: int
    is_encrypted: bool
    is_extra_encrypted: bool
    extra_dejson: Any
