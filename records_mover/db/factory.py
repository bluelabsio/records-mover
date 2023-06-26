from .driver import GenericDBDriver, DBDriver
import sqlalchemy


def db_driver(db: sqlalchemy.engine.Engine,
              **kwargs) -> DBDriver:
    engine: sqlalchemy.engine.Engine = db.engine
    engine_name: str = engine.name

    if engine_name == 'vertica':
        from .vertica.vertica_db_driver import VerticaDBDriver

        return VerticaDBDriver(db, **kwargs)
    elif engine_name == 'redshift':
        from .redshift.redshift_db_driver import RedshiftDBDriver

        return RedshiftDBDriver(db, **kwargs)
    elif engine_name == 'bigquery':
        from .bigquery.bigquery_db_driver import BigQueryDBDriver

        return BigQueryDBDriver(db, **kwargs)
    elif engine_name == 'postgresql':
        from .postgres.postgres_db_driver import PostgresDBDriver

        return PostgresDBDriver(db, **kwargs)
    elif engine_name == 'mysql':
        from .mysql.mysql_db_driver import MySQLDBDriver

        return MySQLDBDriver(db, **kwargs)
    else:
        return GenericDBDriver(db, **kwargs)
