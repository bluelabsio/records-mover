from .vertica.vertica_db_driver import VerticaDBDriver
from .redshift.redshift_db_driver import RedshiftDBDriver
from .bigquery.bigquery_db_driver import BigQueryDBDriver
from .postgres.postgres_db_driver import PostgresDBDriver
from .driver import DBDriver
import sqlalchemy
from typing import Union


def db_driver(db: Union[sqlalchemy.engine.Engine,
                        sqlalchemy.engine.Connection],
              **kwargs) -> DBDriver:
    engine: sqlalchemy.engine.Engine = db.engine

    if engine.name == 'vertica':
        return VerticaDBDriver(db, **kwargs)
    elif engine.name == 'redshift':
        return RedshiftDBDriver(db, **kwargs)
    elif engine.name == 'bigquery':
        return BigQueryDBDriver(db, **kwargs)
    elif engine.name == 'postgresql':
        return PostgresDBDriver(db, **kwargs)
    else:
        return DBDriver(db, **kwargs)
