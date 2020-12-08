import logging
import io
from .base_records_test import BaseRecordsIntegrationTest
from ..datetime_cases import (
    DATE_CASES, DATETIMETZ_CASES, DATETIME_CASES, TIMEONLY_CASES, create_sample,
    SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY, SAMPLE_HOUR, SAMPLE_MINUTE, SAMPLE_SECOND
)
from records_mover.records import RecordsSchema, RecordsFormat
from records_mover.records.schema.field import FieldType

logger = logging.getLogger(__name__)


class RecordsLoadDatetimeIntegrationTest(BaseRecordsIntegrationTest):

    def load(self,
             format_string: str,
             column_name: str,
             field_type: FieldType) -> None:
        variant_for_db = {
            'redshift': 'bluelabs',
        }
        records_schema: RecordsSchema = RecordsSchema.from_data({
            'schema': 'bltypes/v1',
            'fields': {
                column_name: {
                    'type': field_type,
                }
            },
        })
        targets = self.records.targets
        sources = self.records.sources
        fileobj = io.BytesIO(create_sample(format_string).encode('utf-8'))
        records_format = RecordsFormat(variant=variant_for_db[self.engine.name],
                                       hints={
                                           'dateformat': format_string,
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
        self.records.move(source, target)

    def test_load_date(self) -> None:
        for dateformat in DATE_CASES:
            self.load(format_string=dateformat,
                      column_name='date',
                      field_type='date')
            out = self.engine.execute('SELECT date '
                                      f'from {self.schema_name}.{self.table_name}')
            ret_all = out.fetchall()
            assert 1 == len(ret_all)
            ret = ret_all[0]
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
