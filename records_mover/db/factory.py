# flake8: noqa

from .driver import GenericDBDriver, DBDriver
import sqlalchemy
from typing import Union, Optional
from ..check_db_conn_engine import check_db_conn_engine


def db_driver(db: Optional[Union[sqlalchemy.engine.Engine,
                           sqlalchemy.engine.Connection]],
              db_conn: Optional[sqlalchemy.engine.Connection] = None,
              db_engine: Optional[sqlalchemy.engine.Engine] = None,
              **kwargs) -> DBDriver:
    db, db_conn, db_engine = check_db_conn_engine(db=db, db_conn=db_conn, db_engine=db_engine)
    engine_name: str = db_engine.name

    if engine_name == 'vertica':
        from .vertica.vertica_db_driver import VerticaDBDriver

        return VerticaDBDriver(db=db, db_conn=db_conn, db_engine=db_engine, **kwargs)
    elif engine_name == 'redshift':
        from .redshift.redshift_db_driver import RedshiftDBDriver

        return RedshiftDBDriver(db=db, db_conn=db_conn, db_engine=db_engine, **kwargs)
    elif engine_name == 'bigquery':
        from .bigquery.bigquery_db_driver import BigQueryDBDriver

        return BigQueryDBDriver(db=db, db_conn=db_conn, db_engine=db_engine, **kwargs)
    elif engine_name == 'postgresql':
        from .postgres.postgres_db_driver import PostgresDBDriver

        return PostgresDBDriver(db=db, db_conn=db_conn, db_engine=db_engine, **kwargs)
    elif engine_name == 'mysql':
        from .mysql.mysql_db_driver import MySQLDBDriver

        return MySQLDBDriver(db=db, db_conn=db_conn, db_engine=db_engine, **kwargs)
    else:
        return GenericDBDriver(db=db, db_conn=db_conn, db_engine=db_engine, **kwargs)
