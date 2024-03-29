from records_mover.db.quoting import quote_schema_and_table
from records_mover.utils.retry import bigquery_retry
from sqlalchemy import Table, MetaData
from sqlalchemy.schema import DropTable
import logging

logger = logging.getLogger(__name__)


class RecordsDatabaseFixture:
    def quote_schema_and_table(self, schema, table):
        return quote_schema_and_table(None,
                                      schema=schema,
                                      table=table,
                                      db_engine=self.engine)

    def __init__(self, db_engine, schema_name, table_name):
        self.engine = db_engine
        self.schema_name = schema_name
        self.table_name = table_name

    @bigquery_retry()
    def drop_table_if_exists(self, schema, table):
        table_to_drop = Table(table, MetaData(), schema=schema)
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(DropTable(table_to_drop, if_exists=True))

    def tear_down(self):
        self.drop_table_if_exists(self.schema_name, f"{self.table_name}_frozen")
        self.drop_table_if_exists(self.schema_name, self.table_name)

    @bigquery_retry()
    def bring_up(self):
        if self.engine.name == 'redshift':
            create_tables = f"""
              CREATE TABLE {self.quote_schema_and_table(self.schema_name, self.table_name)} AS
              SELECT 123 AS num,
                     '123' AS numstr,
                     'foo' AS str,
                     ',' AS comma,
                     '"' AS doublequote,
                     '","' AS quotecommaquote,
                     '* SQL unload would generate multiple files (one for each slice/part)\n* Filecat would produce a single data file' AS newlinestr,
                     '2000-01-01'::DATE AS date,
                     '00:00:00' AS "time",
                     '2000-01-02 12:34:56.789012'::TIMESTAMP AS timestamp,
                     '2000-01-02 12:34:56.789012 US/Eastern'::TIMESTAMPTZ as timestamptz;
"""  # noqa
        elif self.engine.name == 'vertica':
            create_tables = f"""
              CREATE TABLE {self.quote_schema_and_table(self.schema_name, self.table_name)} AS
              SELECT 123 AS num,
                    '123' AS numstr,
                    'foo' AS str,
                    ',' AS comma,
                    '"' AS doublequote,
                    '","' AS quotecommaquote,
                    E'* SQL unload would generate multiple files (one for each slice/part)\n* Filecat would produce a single data file' AS newlinestr,
                    '2000-01-01'::DATE AS date,
                    '00:00:00'::TIME AS "time",
                    '2000-01-02 12:34:56.789012'::TIMESTAMP AS timestamp,
                    '2000-01-02 12:34:56.789012 US/Eastern'::TIMESTAMPTZ as timestamptz;
"""  # noqa
        elif self.engine.name == 'bigquery':
            create_tables = f"""
              CREATE TABLE {self.quote_schema_and_table(self.schema_name, self.table_name)} AS
              SELECT 123 AS num,
                    '123' AS numstr,
                    'foo' AS str,
                    ',' AS comma,
                    '"' AS doublequote,
                    '","' AS quotecommaquote,
                    '* SQL unload would generate multiple files (one for each slice/part)\\n* Filecat would produce a single data file' AS newlinestr,
                    cast('2000-01-01' as DATE) AS date,
                    cast('00:00:00' as TIME) AS time,
                    cast('2000-01-02 12:34:56.789012' AS DATETIME) AS timestamp,
                    cast('2000-01-02 12:34:56.789012 US/Eastern' AS TIMESTAMP) as timestamptz;
"""  # noqa
        elif self.engine.name == 'postgresql':
            create_tables = f"""
              CREATE TABLE {self.quote_schema_and_table(self.schema_name, self.table_name)} AS
              SELECT 123 AS num,
                     '123' AS numstr,
                     'foo' AS str,
                     ',' AS comma,
                     '"' AS doublequote,
                     '","' AS quotecommaquote,
                     '* SQL unload would generate multiple files (one for each slice/part)\n* Filecat would produce a single data file' AS newlinestr,
                     '2000-01-01'::DATE AS date,
                     '00:00:00'::TIME AS "time",
                     '2000-01-02 12:34:56.789012'::TIMESTAMP AS "timestamp",
                     '2000-01-02 12:34:56.789012 US/Eastern'::TIMESTAMPTZ as "timestamptz";
"""  # noqa
        elif self.engine.name == 'mysql':
            create_tables = f"""
              CREATE TABLE {self.quote_schema_and_table(self.schema_name, self.table_name)} AS
              SELECT 123 AS num,
                     '123' AS numstr,
                     'foo' AS str,
                     ',' AS comma,
                     '"' AS doublequote,
                     '","' AS quotecommaquote,
                     '* SQL unload would generate multiple files (one for each slice/part)\n* Filecat would produce a single data file' AS newlinestr,
                     DATE '2000-01-01' AS "date",
                     TIME '00:00:00' AS "time",
                     TIMESTAMP '2000-01-02 12:34:56.789012' AS "timestamp",
                     TIMESTAMP '2000-01-02 12:34:56.789012-05' AS "timestamptz";
"""  # noqa
        else:
            raise NotImplementedError(f"Please teach me how to integration test {self.engine.name}")
        with self.engine.connect() as connection:
            with connection.begin():
                connection.exec_driver_sql(create_tables)
