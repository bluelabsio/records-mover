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
    """
    This function was added because, in previous versions of SQL Alchemy, you could execute SQL against either
    Engines or Connections and not worry very much about the difference between the two.  Since version 2, this
    is no longer the case.

    Because of the interchangeability between Engines and Connections, most of our code accepted a rather ambiguous
    'db' argument, which could be either an Engine or a Connection.  This function is intended to be used to disabiguate
    the two, create engines from connections where possible, and warn users to stop using the 'db' argument.

    Args:
        db: Deprecated argument. Use db_conn for Connection objects and db_engine for Engine objects.
        db_conn: Optional SQLAlchemy Connection object.
        db_engine: Optional SQLAlchemy Engine object.

    Returns:
        A tuple containing the Connection or Engine 'db' object provided as input, the Connection object if provided,
        and the Engine object.

    Raises:
        ValueError: If none of db, db_conn, or db_engine are provided as arguments.
    """
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
        print("db_engine is not provided, so we're assigning db_engine to db_conn.engine")
        db_engine = db_conn.engine  # type: ignore[union-attr]
    return (db, db_conn, db_engine)  # type: ignore[return-value]
