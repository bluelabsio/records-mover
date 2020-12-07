import logging
import io
from .base_records_test import BaseRecordsIntegrationTest
from ..datetime_cases import (
    DATE_CASES, DATETIMETZ_CASES, DATETIME_CASES, TIMEONLY_CASES, create_sample,
    SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY, SAMPLE_HOUR, SAMPLE_MINUTE, SAMPLE_SECOND
)
from records_mover.records import RecordsSchema, RecordsFormat

logger = logging.getLogger(__name__)


class RecordsLoadDatetimeIntegrationTest(BaseRecordsIntegrationTest):
    def test_load_date(self) -> None:
        variant_for_db = {
            'redshift': 'bluelabs',
        }
        for dateformat in DATE_CASES:
            records_schema: RecordsSchema = RecordsSchema.from_data({
                'schema': 'bltypes/v1',
                'fields': {
                    'date': {
                        'type': 'date',
                    }
                },
            })
            targets = self.records.targets
            sources = self.records.sources
            fileobj = io.BytesIO(create_sample(dateformat).encode('utf-8'))
            records_format = RecordsFormat(variant=variant_for_db[self.engine.name],
                                           hints={
                                               'dateformat': dateformat,
                                               'compression': None,
                                           })
            source = sources.fileobjs(target_names_to_input_fileobjs={
                                        'test': fileobj
                                      },
                                      records_schema=records_schema,
                                      records_format=records_format)
            target = targets.table(schema_name=self.schema_name,
                                   table_name=self.table_name,
                                   db_engine=self.engine)
            out = self.records.move(source, target)
            out = self.engine.execute('SELECT date '
                                      f'from {self.schema_name}.{self.table_name}')
            ret = out.fetchall()
            assert 1 == len(ret)
            date = ret['date']
            self.assertEqual(date.year, SAMPLE_YEAR)
            self.assertEqual(date.month, SAMPLE_MONTH)
            self.assertEqual(date.day, SAMPLE_DAY)


    def test_load_timeonly(self):
        raise

    def test_load_datetimetz(self):
        raise

    def test_load_datetime(self):
        raise
