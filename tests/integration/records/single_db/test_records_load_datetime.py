import logging
import io
import datetime
from .base_records_test import BaseRecordsIntegrationTest
from ..datetime_cases import (
    DATE_CASES, DATETIMETZ_CASES, DATETIME_CASES, TIMEONLY_CASES, create_sample,
    SAMPLE_YEAR, SAMPLE_MONTH, SAMPLE_DAY, SAMPLE_HOUR, SAMPLE_MINUTE, SAMPLE_SECOND
)
from records_mover.records import RecordsSchema, RecordsFormat, PartialRecordsHints
from records_mover.records.schema.field.field_types import FieldType
from sqlalchemy import text

logger = logging.getLogger(__name__)


class RecordsLoadDatetimeIntegrationTest(BaseRecordsIntegrationTest):
    def load(self,
             hint_name: str,
             format_string: str,
             column_name: str,
             field_type: FieldType,
             addl_hints: PartialRecordsHints = {}) -> None:
        variant_for_db = {
            'redshift': 'bluelabs',
            'vertica': 'vertica',
            'postgresql': 'bluelabs',
            'mysql': 'bluelabs',
            'bigquery': 'bigquery',
        }
        records_schema: RecordsSchema = RecordsSchema.from_data({
            'schema': 'bltypes/v1',
            'fields': {
                column_name: {
                    'type': field_type,
                },
            },
        })
        targets = self.records.targets
        sources = self.records.sources
        sample = create_sample(format_string)
        logger.info(f'Testing date/time load with {sample} from format {format_string}')
        fileobj = io.BytesIO(sample.encode('utf-8'))
        records_format = RecordsFormat(variant=variant_for_db[self.engine.name],
                                       hints={
                                           hint_name: format_string,  # type: ignore
                                           'compression': None,
                                           'header-row': False,
                                           **addl_hints,
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

    def pull_result(self,
                    column_name: str) -> datetime.datetime:
        with self.engine.connect() as connection:
            with connection.begin():
                out = connection.execute(text(
                        f'SELECT {column_name} '
                        f'from {self.schema_name}.{self.table_name}'))
                ret_all = out.fetchall()
        assert 1 == len(ret_all)
        ret = ret_all[0]
        ret_mapping = ret._mapping
        return ret_mapping[column_name]

    def database_provides_times_as_timedeltas(self) -> bool:
        return self.engine.name == 'mysql'

    def test_load_date(self) -> None:
        for dateformat in DATE_CASES:
            addl_hints: PartialRecordsHints = {}
            if self.engine.name in ['vertica', 'mysql', 'redshift',
                                    'bigquery']:
                # Use something more compatible with Pandas that is
                # still compatible when the above databases, which are
                # limited in what they can accept...
                addl_hints = {
                    'datetimeformat': f"{dateformat} HH:MI:SS",
                    'datetimeformattz': f"{dateformat} HH:MI:SSOF",
                    'timeonlyformat': "HH:MI:SS",
                }
            self.load(hint_name='dateformat',
                      format_string=dateformat,
                      column_name='date',
                      field_type='date',
                      # ensure a Pandas-compatible format in case
                      # database doesn't support hints directly
                      addl_hints=addl_hints)
            date_obj = self.pull_result(column_name='date')
            if isinstance(date_obj, str):
                # If Pandas was required, the date type will be
                # simplified to string as pandas doesn't support
                # date-only columns
                self.assertEqual(date_obj, create_sample(dateformat),
                                 f"Problem loading date with dateformat {dateformat}")
            else:
                self.assertEqual(date_obj.year, SAMPLE_YEAR)
                self.assertEqual(date_obj.month, SAMPLE_MONTH)
                self.assertEqual(date_obj.day, SAMPLE_DAY)

    def database_has_no_time_type(self) -> bool:
        return self.engine.name == 'redshift'

    def test_load_timeonly(self) -> None:
        if self.engine.name == 'redshift' and not self.has_scratch_s3_bucket():
            # https://github.com/bluelabsio/records-mover/issues/141
            logger.warning('Records Mover does not yet support Redshift TIME '
                           'fields, nor is it not smart enough to format Pandas '
                           'time types to the Redshift VARCHAR(8) columns it creates')
            return
        for timeformat in TIMEONLY_CASES:
            self.load(hint_name='timeonlyformat',
                      format_string=timeformat,
                      column_name='time',
                      field_type='time')
            time_obj = self.pull_result(column_name='time')
            if self.database_has_no_time_type():
                self.assertEqual(time_obj, create_sample(timeformat))
            elif self.database_provides_times_as_timedeltas():
                assert isinstance(time_obj, datetime.timedelta)
                minutes = SAMPLE_HOUR*60 + SAMPLE_MINUTE
                if 'SS' in timeformat:
                    seconds = minutes*60 + SAMPLE_SECOND
                else:
                    seconds = minutes*60
                self.assertEqual(time_obj.seconds, seconds)

            else:
                self.assertEqual(time_obj.hour, SAMPLE_HOUR)
                self.assertEqual(time_obj.minute, SAMPLE_MINUTE)
                if 'SS' in timeformat:
                    self.assertEqual(time_obj.second, SAMPLE_SECOND)
                else:
                    self.assertEqual(time_obj.second, 0)

    def test_load_datetimetz(self) -> None:
        for datetimetzformat in DATETIMETZ_CASES:
            self.load(hint_name='datetimeformattz',
                      format_string=datetimetzformat,
                      column_name='datetimetz',
                      field_type='datetimetz')
            datetime = self.pull_result(column_name='datetimetz')
            self.assertEqual(datetime.year, SAMPLE_YEAR)
            self.assertEqual(datetime.month, SAMPLE_MONTH)
            self.assertEqual(datetime.day, SAMPLE_DAY)
            self.assertEqual(datetime.hour, SAMPLE_HOUR)
            self.assertEqual(datetime.minute, SAMPLE_MINUTE)
            if 'SS' in datetimetzformat:
                self.assertEqual(datetime.second, SAMPLE_SECOND)
            else:
                self.assertEqual(datetime.second, 0)

    def test_load_datetime(self) -> None:
        for datetimeformat in DATETIME_CASES:
            self.load(hint_name='datetimeformat',
                      format_string=datetimeformat,
                      column_name='datetime',
                      field_type='datetime')
            datetime_obj = self.pull_result(column_name='datetime')
            self.assertEqual(datetime_obj.year, SAMPLE_YEAR)
            self.assertEqual(datetime_obj.month, SAMPLE_MONTH)
            self.assertEqual(datetime_obj.day, SAMPLE_DAY)
            self.assertEqual(datetime_obj.hour, SAMPLE_HOUR)
            self.assertEqual(datetime_obj.minute, SAMPLE_MINUTE)
            if 'SS' in datetimeformat:
                self.assertEqual(datetime_obj.second, SAMPLE_SECOND)
            else:
                self.assertEqual(datetime_obj.second, 0)
