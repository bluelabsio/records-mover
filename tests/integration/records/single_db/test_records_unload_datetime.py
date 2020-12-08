import logging
import io
import tempfile
import pathlib
from .base_records_test import BaseRecordsIntegrationTest
from records_mover.db.quoting import quote_schema_and_table
from records_mover.records.records_directory import RecordsDirectory
from records_mover.records.records_format import BaseRecordsFormat
from records_mover.utils.retry import bigquery_retry
from ..records_database_fixture import RecordsDatabaseFixture
from ..datetime_cases import (
    DATE_CASES, DATETIMETZ_CASES, DATETIME_CASES, TIMEONLY_CASES, create_sample,
    SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY, SAMPLE_HOUR, SAMPLE_MINUTE, SAMPLE_SECOND, SAMPLE_OFFSET, SAMPLE_LONG_TZ
)
from records_mover.records import (
    RecordsSchema, RecordsFormat, PartialRecordsHints
)
from records_mover.records.records_format import DelimitedRecordsFormat
from records_mover.records.schema.field.field_types import FieldType

logger = logging.getLogger(__name__)


VARIANT_FOR_DB = {
    'redshift': 'bluelabs',
    'vertica': 'vertica',
    'postgresql': 'bluelabs',
    'mysql': 'bluelabs',
    'bigquery': 'bigquery',
}


class RecordsUnloadDatetimeIntegrationTest(BaseRecordsIntegrationTest):
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
              SELECT TIMESTAMP '{SAMPLE_YEAR}-{SAMPLE_MONTH}-{SAMPLE_DAY} {SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d}{SAMPLE_OFFSET}' AS "timestamptz";
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

    def drop_tables(self):
        logger.info('Dropping tables...')
        self.drop_table_if_exists(self.schema_name, f"{self.table_name}_frozen")
        self.drop_table_if_exists(self.schema_name, self.table_name)

    def tearDown(self):
        super().tearDown()
        self.drop_tables()

    def unload(self,
               column_name: str,
               records_format: BaseRecordsFormat) -> str:
        targets = self.records.targets
        sources = self.records.sources
        with tempfile.TemporaryDirectory() as directory_name:
            source = sources.table(schema_name=self.schema_name,
                                   table_name=self.table_name,
                                   db_engine=self.engine)
            directory_url = pathlib.Path(directory_name).as_uri() + '/'
            target = targets.directory_from_url(output_url=directory_url,
                                                records_format=records_format)
            self.records.move(source, target)
            from records_mover.records import mover
            mover.logger.info('Done with move')
            directory_loc = self.session.directory_url(directory_url)
            records_dir = RecordsDirectory(records_loc=directory_loc)
            with tempfile.NamedTemporaryFile() as t:
                output_url = pathlib.Path(t.name).as_uri()
                output_loc = self.session.file_url(output_url)
                records_dir.save_to_url(output_loc)
                return output_loc.string_contents()

    def test_unload_date(self) -> None:
        self.createDateTable()
        for dateformat in DATE_CASES:
            addl_hints: PartialRecordsHints = {}
            if self.engine.name == 'redshift':
                if dateformat != 'YYYY-MM-DD':
                    # this is the only format supported by Redshift on
                    # export, so we're going to need to be sure to use
                    # hints that work in Pandas
                    addl_hints = {
                        'datetimeformat': f'{dateformat} HH24:MI:SS',
                        'datetimeformattz': f'{dateformat} HH:MI:SS',
                    }
            records_format = RecordsFormat(variant=VARIANT_FOR_DB[self.engine.name],
                                           hints={
                                               'dateformat': dateformat,
                                               'compression': None,
                                               'header-row': False,
                                               **addl_hints,  # type: ignore
                                           })
            csv_text = self.unload(column_name='date',
                                   records_format=records_format)
            self.assertEqual(csv_text, create_sample(dateformat) + "\n",
                             f"from dateformat {dateformat} and addl_hints {addl_hints}")

    def test_unload_datetime(self) -> None:
        self.createDateTimeTable()
        matching_dateformat = {
            'YYYY-MM-DD HH:MI:SS': 'YYYY-MM-DD',
            'YYYY-MM-DD HH12:MI AM': 'YYYY-MM-DD',
            'MM/DD/YY HH24:MI': 'MM/DD/YY',
        }
        for datetimeformat in DATETIME_CASES:
            addl_hints: PartialRecordsHints = {}
            if self.engine.name == 'redshift':
                if datetimeformat != 'YYYY-MM-DD HH24:MI:SS':
                    # this is the only format supported by Redshift on
                    # export, so we're going to need to be sure to use
                    # hints that work in Pandas - i.e.,
                    addl_hints = {
                        'dateformat': matching_dateformat[datetimeformat],
                        'datetimeformattz': datetimeformat,
                    }
                    if 'AM' in datetimeformat:
                        # TODO: Add a GitHub issue for this
                        logger.warning('Cannot export this dateformat using Pandas or Redshift--'
                                       'skipping test')
                        continue
            records_format = RecordsFormat(variant=VARIANT_FOR_DB[self.engine.name],
                                           hints={
                                               'datetimeformat': datetimeformat,
                                               'compression': None,
                                               'header-row': False,
                                               **addl_hints,  # type: ignore
                                           })
            csv_text = self.unload(column_name='timestamp',
                                   records_format=records_format)
            self.assertIn(csv_text, [create_sample(datetimeformat) + "\n",
                                     # TODO: Should this be necessary?
                                     create_sample(datetimeformat) + ".000000\n",
                                     # TODO: Should this be necessary?
                                     create_sample(datetimeformat) +
                                     f":{SAMPLE_SECOND:02d}.000000\n"],
                          f"from datetimeformat {datetimeformat} and addl_hints {addl_hints}")

    def test_unload_datetimetz(self) -> None:
        self.createDateTimeTzTable()
        matching_dateformat = {
            'YYYY-MM-DD HH:MI:SS': 'YYYY-MM-DD',
            'YYYY-MM-DD HH12:MI AM': 'YYYY-MM-DD',
            'MM/DD/YY HH24:MI': 'MM/DD/YY',
            'YYYY-MM-DD HH24:MI:SSOF': 'YYYY-MM-DD',
        }
        for datetimeformattz in DATETIMETZ_CASES:
            addl_hints: PartialRecordsHints = {}
            if self.engine.name == 'redshift':
                if datetimeformattz != 'YYYY-MM-DD HH:MI:SSOF':
                    # this is the only format supported by Redshift on
                    # export, so we're going to need to be sure to use
                    # hints that work in Pandas - i.e.,
                    addl_hints = {
                        'dateformat': matching_dateformat[datetimeformattz],
                        'datetimeformat': datetimeformattz.replace('OF', ''),
                    }
                    if 'AM' in datetimeformattz:
                        # TODO: Add a GitHub issue for this
                        logger.warning('Cannot export this dateformattz using Pandas or Redshift--'
                                       'skipping test')
                        continue
            records_format = RecordsFormat(variant=VARIANT_FOR_DB[self.engine.name],
                                           hints={
                                               'datetimeformattz': datetimeformattz,
                                               'compression': None,
                                               'header-row': False,
                                               **addl_hints,  # type: ignore
                                           })
            csv_text = self.unload(column_name='timestamptz',
                                   records_format=records_format)
            self.assertIn(csv_text, [create_sample(datetimeformattz) + "\n",
                                     create_sample(datetimeformattz).replace('-00', '+00') + "\n",
                                     # TODO: Should this be necessary?
                                     create_sample(datetimeformattz) + ".000000\n",
                                     # TODO: Should this be necessary?
                                     create_sample(datetimeformattz).replace('-00', '.000000+0000\n'),
                                     # TODO: Should this be necessary?
                                     create_sample(datetimeformattz) +
                                     f":{SAMPLE_SECOND:02d}.000000\n"
                                     ],
                          f"from datetimeformattz {datetimeformattz} and addl_hints {addl_hints}")

    def test_unload_timeonly(self) -> None:
        raise