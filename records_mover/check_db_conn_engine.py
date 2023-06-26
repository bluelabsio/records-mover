# flake8: noqa

import sqlalchemy
from typing import Union, Optional, Tuple
import warnings


def check_db_conn_engine(db: Optional[Union[sqlalchemy.engine.Engine,
                                            sqlalchemy.engine.Connection]] = None,
                         db_conn: Optional[sqlalchemy.engine.Connection] = None,
                         db_engine: Optional[sqlalchemy.engine.Engine] = None) -> \
    Tuple[Optional[Union[sqlalchemy.engine.Engine,
                         sqlalchemy.engine.Connection]],
          Optional[sqlalchemy.engine.Connection],
          sqlalchemy.engine.Engine]:
    if db:
        warnings.warn("The db argument is deprecated and will be"
                      "removed in future releases.\n"
                      "Please use db_conn for Connection objects and db_engine for Engine"
                      "objects.",
                      DeprecationWarning)
    if not (db or db_conn or db_engine):
        raise ValueError("Either db, db_conn, or db_engine must be provided as arguments")
    if isinstance(db, sqlalchemy.engine.Connection) and not db_conn:
        db_conn = db
    if isinstance(db, sqlalchemy.engine.Engine) and not db_engine:
        db_engine = db
    if not db_engine:
        print("db_engine is not provided, so we're creating one from db_conn")
        db_engine = db_conn.engine  # type: ignore[union-attr]
    return (db, db_conn, db_engine)  # type: ignore[return-value]
