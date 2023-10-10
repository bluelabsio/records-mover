import logging
import sqlalchemy
from typing import Optional

logger = logging.getLogger(__name__)


def composable_get_db_conn(self) -> sqlalchemy.engine.Connection:
    if self._db_conn is None:
        self._db_conn = self.db_engine.connect()
        self.conn_opened_here = True
        logger.debug(f"Opened connection to database within {self} because none was provided.")
    return self._db_conn


def composable_set_db_conn(self, db_conn: Optional[sqlalchemy.engine.Connection]) -> None:
    self._db_conn = db_conn


def composable_del_db_conn(self) -> None:
    if self.conn_opened_here:
        self.db_conn.close()
        self.db_conn = None
