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
    SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY, SAMPLE_HOUR, SAMPLE_MINUTE, SAMPLE_SECOND
)
from records_mover.records import (
    RecordsSchema, RecordsFormat, PartialRecordsHints
)
from records_mover.records.records_format import DelimitedRecordsFormat
from records_mover.records.schema.field.field_types import FieldType

logger = logging.getLogger(__name__)


class RecordsUnloadDatetimeIntegrationTest(BaseRecordsIntegrationTest):
    def quote_schema_and_table(self, schema, table):
        return quote_schema_and_table(self.engine, schema, table)

    @bigquery_retry()
    def drop_table_if_exists(self, schema, table):
        sql = f"DROP TABLE IF EXISTS {self.quote_schema_and_table(schema, table)}"
        self.engine.execute(sql)

    def createDateTable(self) -> None:
        if self.engine.name == 'redshift':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT '1983-01-02'::DATE AS date;
"""  # noqa
        elif self.engine.name == 'vertica':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT '1983-01-02'::DATE AS date;
"""  # noqa
        elif self.engine.name == 'bigquery':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT cast('1983-01-02' as DATE) AS date;
"""  # noqa
        elif self.engine.name == 'postgresql':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT '1983-01-02'::DATE AS date;
"""  # noqa
        elif self.engine.name == 'mysql':
            create_tables = f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT DATE '1983-01-02' AS "date";
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
            variant_for_db = {
                'redshift': 'bluelabs',
                'vertica': 'vertica',
                'postgresql': 'bluelabs',
                'mysql': 'bluelabs',
                'bigquery': 'bigquery',
            }
            records_format = RecordsFormat(variant=variant_for_db[self.engine.name],
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
        raise

    def test_unload_datetimetz(self) -> None:
        raise

    def test_unload_timeonly(self) -> None:
        raise
