from records_mover.db.quoting import quote_schema_and_table
from records_mover.utils.retry import bigquery_retry
from .datetime_cases import (
    DATE_CASES, DATETIMETZ_CASES, DATETIME_CASES, TIMEONLY_CASES, create_sample,
    SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY, SAMPLE_HOUR, SAMPLE_MINUTE, SAMPLE_SECOND, SAMPLE_OFFSET, SAMPLE_LONG_TZ
)
from sqlalchemy.engine import Engine
import logging

logger = logging.getLogger(__name__)


class RecordsDatetimeFixture:
    def __init__(self, engine: Engine, schema_name: str, table_name: str):
        self.engine = engine
        self.schema_name = schema_name
        self.table_name = table_name

    def quote_schema_and_table(self, schema, table):
        return quote_schema_and_table(self.engine, schema, table)

    @bigquery_retry()
    def drop_table_if_exists(self, schema, table):
        sql = f"DROP TABLE IF EXISTS {self.quote_schema_and_table(schema, table)}"
        self.engine.execute(sql)

    def createDateTimeTzTable(self) -> None:
        if self.engine.name == 'redshift':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT '{SAMPLE_YEAR}-{SAMPLE_MONTH}-{SAMPLE_DAY} {SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d} {SAMPLE_LONG_TZ}'::TIMESTAMPTZ as timestamptz;
"""  # noqa
        elif self.engine.name == 'vertica':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT '{SAMPLE_YEAR}-{SAMPLE_MONTH}-{SAMPLE_DAY} {SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d} {SAMPLE_LONG_TZ}'::TIMESTAMPTZ as timestamptz;
"""  # noqa
        elif self.engine.name == 'bigquery':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT cast('{SAMPLE_YEAR}-{SAMPLE_MONTH}-{SAMPLE_DAY} {SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d} {SAMPLE_LONG_TZ}' AS TIMESTAMP) as timestamptz;
"""  # noqa
        elif self.engine.name == 'postgresql':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT '{SAMPLE_YEAR}-{SAMPLE_MONTH}-{SAMPLE_DAY} {SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d} {SAMPLE_LONG_TZ}'::TIMESTAMPTZ as "timestamptz";
"""  # noqa
        elif self.engine.name == 'mysql':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT TIMESTAMP '{SAMPLE_YEAR}-{SAMPLE_MONTH:02d}-{SAMPLE_DAY:02d} {SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d}.000000{SAMPLE_OFFSET}' AS "timestamptz";
"""  # noqa
        else:
            raise NotImplementedError(f"Please teach me how to integration test {self.engine.name}")
        self.engine.execute(create_tables)

    def createDateTimeTable(self) -> None:
        if self.engine.name == 'redshift':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT '{SAMPLE_YEAR}-{SAMPLE_MONTH}-{SAMPLE_DAY} {SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d}'::TIMESTAMP AS timestamp;
"""  # noqa
        elif self.engine.name == 'vertica':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT '{SAMPLE_YEAR}-{SAMPLE_MONTH}-{SAMPLE_DAY} {SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d}'::TIMESTAMP AS timestamp;
"""  # noqa
        elif self.engine.name == 'bigquery':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT cast('{SAMPLE_YEAR}-{SAMPLE_MONTH}-{SAMPLE_DAY} {SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d}' AS DATETIME) AS timestamp;
"""  # noqa
        elif self.engine.name == 'postgresql':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT '{SAMPLE_YEAR}-{SAMPLE_MONTH}-{SAMPLE_DAY} {SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d}'::TIMESTAMP AS "timestamp";
"""  # noqa
        elif self.engine.name == 'mysql':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT TIMESTAMP '{SAMPLE_YEAR}-{SAMPLE_MONTH}-{SAMPLE_DAY} {SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d}' AS "timestamp";
"""  # noqa
        else:
            raise NotImplementedError(f"Please teach me how to integration test {self.engine.name}")
        self.engine.execute(create_tables)

    @bigquery_retry()
    def createDateTable(self) -> None:
        if self.engine.name == 'redshift':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT '{SAMPLE_YEAR}-{SAMPLE_MONTH}-{SAMPLE_DAY}'::DATE AS date;
"""  # noqa
        elif self.engine.name == 'vertica':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT '{SAMPLE_YEAR}-{SAMPLE_MONTH}-{SAMPLE_DAY}'::DATE AS date;
"""  # noqa
        elif self.engine.name == 'bigquery':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT cast('{SAMPLE_YEAR}-{SAMPLE_MONTH}-{SAMPLE_DAY}' as DATE) AS date;
"""  # noqa
        elif self.engine.name == 'postgresql':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT '{SAMPLE_YEAR}-{SAMPLE_MONTH}-{SAMPLE_DAY}'::DATE AS date;
"""  # noqa
        elif self.engine.name == 'mysql':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT DATE '{SAMPLE_YEAR}-{SAMPLE_MONTH}-{SAMPLE_DAY}' AS "date";
"""  # noqa
        else:
            raise NotImplementedError(f"Please teach me how to integration test {self.engine.name}")
        self.engine.execute(create_tables)

    @bigquery_retry()
    def createTimeTable(self):
        if self.engine.name == 'redshift':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT '{SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d}' AS "time";
"""  # noqa
        elif self.engine.name == 'vertica':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT '{SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d}'::TIME AS "time";
"""  # noqa
        elif self.engine.name == 'bigquery':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT cast('{SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d}' as TIME) AS time;
"""  # noqa
        elif self.engine.name == 'postgresql':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT '{SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d}'::TIME AS "time";
"""  # noqa
        elif self.engine.name == 'mysql':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT TIME '{SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d}' AS "time";
"""  # noqa
        else:
            raise NotImplementedError(f"Please teach me how to integration test {self.engine.name}")
        self.engine.execute(create_tables)

    def drop_tables(self):
        logger.info('Dropping tables...')
        self.drop_table_if_exists(self.schema_name, f"{self.table_name}_frozen")
        self.drop_table_if_exists(self.schema_name, self.table_name)
