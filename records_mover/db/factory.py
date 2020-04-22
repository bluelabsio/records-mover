from .driver import DBDriver
import sqlalchemy
from typing import Union


def db_driver(db: Union[sqlalchemy.engine.Engine,
                        sqlalchemy.engine.Connection],
              **kwargs) -> DBDriver:
    engine: sqlalchemy.engine.Engine = db.engine

    if engine.name == 'vertica':
        from .vertica.vertica_db_driver import VerticaDBDriver

        return VerticaDBDriver(db, **kwargs)
    elif engine.name == 'redshift':
        from .redshift.redshift_db_driver import RedshiftDBDriver

        return RedshiftDBDriver(db, **kwargs)
    elif engine.name == 'bigquery':
        from .bigquery.bigquery_db_driver import BigQueryDBDriver

        return BigQueryDBDriver(db, **kwargs)
    elif engine.name == 'postgresql':
        from .postgres.postgres_db_driver import PostgresDBDriver

        return PostgresDBDriver(db, **kwargs)
    elif engine.name == 'mysql':
        from .mysql.mysql_db_driver import MySQLDBDriver

        return MySQLDBDriver(db, **kwargs)
    else:
        return DBDriver(db, **kwargs)
