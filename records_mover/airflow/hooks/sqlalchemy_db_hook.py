import sqlalchemy as sa
from records_mover.db import create_sqlalchemy_url
from airflow.hooks import BaseHook


class SqlAlchemyDbHook(BaseHook):
    "Airflow hook to provide a SQLAlchemy engine from an Airflow database connection ID"

    def __init__(self, db_conn_id: str) -> None:
        """
        :param db_conn_id: Airflow connection ID for the database which should be connected to.
        """
        self.db_conn_id = db_conn_id

    def get_conn(self) -> sa.engine.Engine:
        """
        :return: SQLAlchemy engine corresponding to this Airflow database connection ID.
        """
        conn = BaseHook.get_connection(self.db_conn_id)
        db_url = create_sqlalchemy_url(
            {
                'host': conn.host,
                'port': conn.port,
                'database': conn.schema,
                'user': conn.login,
                'password': conn.password,
                'type': conn.extra_dejson.get('type', conn.conn_type.lower()),
            }
        )
        return sa.create_engine(db_url)
