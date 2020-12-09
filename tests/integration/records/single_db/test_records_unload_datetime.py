import logging
import tempfile
import pathlib
from .base_records_test import BaseRecordsIntegrationTest
from records_mover.records.records_directory import RecordsDirectory
from records_mover.records.records_format import BaseRecordsFormat
from ..records_datetime_fixture import RecordsDatetimeFixture
from ..datetime_cases import (
    DATE_CASES, DATETIMETZ_CASES, DATETIME_CASES, TIMEONLY_CASES, create_sample,
    SAMPLE_HOUR, SAMPLE_MINUTE, SAMPLE_SECOND
)
from records_mover.records import RecordsFormat, PartialRecordsHints

logger = logging.getLogger(__name__)


VARIANT_FOR_DB = {
    'redshift': 'bluelabs',
    'vertica': 'vertica',
    'postgresql': 'bluelabs',
    'mysql': 'bluelabs',
    'bigquery': 'bigquery',
}


class RecordsUnloadDatetimeIntegrationTest(BaseRecordsIntegrationTest):
    def setUp(self) -> None:
        super().setUp()
        self.datetime_fixture = RecordsDatetimeFixture(engine=self.engine,
                                                       table_name=self.table_name,
                                                       schema_name=self.schema_name)

    def tearDown(self):
        super().tearDown()
        self.datetime_fixture.drop_tables()

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
            directory_loc = self.session.directory_url(directory_url)
            records_dir = RecordsDirectory(records_loc=directory_loc)
            with tempfile.NamedTemporaryFile() as t:
                output_url = pathlib.Path(t.name).as_uri()
                output_loc = self.session.file_url(output_url)
                records_dir.save_to_url(output_loc)
                return output_loc.string_contents()

    def test_unload_date(self) -> None:
        self.datetime_fixture.createDateTable()
        for dateformat in DATE_CASES:
            addl_hints: PartialRecordsHints = {}
            pandas_compatible_addl_hints: PartialRecordsHints = {
                'datetimeformat': f'{dateformat} HH24:MI:SS',
                'datetimeformattz': f'{dateformat} HH:MI:SS',
            }
            uses_pandas = False
            if self.engine.name == 'redshift':
                if dateformat != 'YYYY-MM-DD':
                    # this is the only format supported by Redshift on
                    # export
                    uses_pandas = True
            elif self.engine.name == 'mysql':
                # mysql has no exporter defined, so everything is via Pandas
                addl_hints = pandas_compatible_addl_hints
            elif self.engine.name == 'postgresql':
                if dateformat == 'YYYY-MM-DD':
                    # ensure we keep other areas of ISO style
                    addl_hints = {
                        'timeonlyformat': 'HH:MI:SS',
                        'datetimeformattz': 'YYYY-MM-DD HH:MI:SS',
                        'datetimeformat': 'YYYY-MM-DD HH:MI:SS',
                    }
                else:
                    uses_pandas = True
            elif self.engine.name == 'vertica':
                # Make sure our '\n' strings below are valid when comparing output
                addl_hints['record-terminator'] = '\n'
                if dateformat != 'YYYY-MM-DD':
                    uses_pandas = True
            elif self.engine.name == 'bigquery':
                # All current export is via Avro
                uses_pandas = True
            if uses_pandas:
                # we're going to need to be sure to use hints that work in Pandas
                addl_hints.update(pandas_compatible_addl_hints)
            records_format = RecordsFormat(variant=VARIANT_FOR_DB[self.engine.name],
                                           hints={
                                               'dateformat': dateformat,
                                               'compression': None,
                                               'header-row': False,
                                               **addl_hints,  # type: ignore
                                           })
            expect_pandas_failure = not self.has_pandas() and uses_pandas
            try:
                csv_text = self.unload(column_name='date',
                                       records_format=records_format)
                self.assertEqual(csv_text, create_sample(dateformat) + "\n",
                                 f"from dateformat {dateformat} and addl_hints {addl_hints}")
            except ModuleNotFoundError as e:
                if 'pandas' in str(e) and expect_pandas_failure:
                    # as expected
                    continue
                else:
                    raise
            self.assertFalse(expect_pandas_failure)

    def test_unload_datetime(self) -> None:
        self.datetime_fixture.createDateTimeTable()
        matching_dateformat = {
            'YYYY-MM-DD HH:MI:SS': 'YYYY-MM-DD',
            'YYYY-MM-DD HH12:MI AM': 'YYYY-MM-DD',
            'MM/DD/YY HH24:MI': 'MM/DD/YY',
            'YYYY-MM-DD HH24:MI:SS': 'YYYY-MM-DD',
        }
        for datetimeformat in DATETIME_CASES:
            addl_hints: PartialRecordsHints = {}
            pandas_compatible_addl_hints: PartialRecordsHints = {
                'dateformat': matching_dateformat[datetimeformat],
                'datetimeformattz': datetimeformat,
            }
            uses_pandas = False
            if self.engine.name == 'redshift':
                if datetimeformat != 'YYYY-MM-DD HH24:MI:SS':
                    # this is the only format supported by Redshift on
                    # export
                    uses_pandas = True
            elif self.engine.name == 'mysql':
                # mysql has no exporter defined, so everything is via Pandas
                uses_pandas = True
            elif self.engine.name == 'postgresql':
                if datetimeformat in ['YYYY-MM-DD HH:MI:SS',
                                      'YYYY-MM-DD HH24:MI:SS']:
                    # ensure we keep other areas of ISO style
                    addl_hints = {
                        'timeonlyformat': 'HH:MI:SS',
                        'dateformat': 'YYYY-MM-DD',
                        'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
                    }
                else:
                    uses_pandas = True
            elif self.engine.name == 'vertica':
                # Make sure our '\n' strings below are valid when comparing output
                addl_hints['record-terminator'] = '\n'
                if datetimeformat not in ['YYYY-MM-DD HH:MI:SS',
                                          'YYYY-MM-DD HH24:MI:SS']:
                    uses_pandas = True
            elif self.engine.name == 'bigquery':
                # All current export is via Avro
                uses_pandas = True
            if uses_pandas:
                # TODO: Should I verify this?
                # We're going to need to be sure to use hints that
                # work in Pandas
                addl_hints.update(pandas_compatible_addl_hints)
                if 'AM' in datetimeformat:
                    # TODO: Add a GitHub issue for this
                    # TODO: Should expect failure and complain if it doesn't fail
                    logger.warning('Cannot export this dateformat using Pandas--'
                                   'skipping test')
                    continue
            records_format = RecordsFormat(variant=VARIANT_FOR_DB[self.engine.name],
                                           hints={
                                               'datetimeformat': datetimeformat,
                                               'compression': None,
                                               'header-row': False,
                                               **addl_hints,  # type: ignore
                                           })
            expect_pandas_failure = not self.has_pandas() and uses_pandas
            try:
                csv_text = self.unload(column_name='timestamp',
                                       records_format=records_format)
                self.assertIn(csv_text, [create_sample(datetimeformat) + "\n",
                                         # TODO: Should this be necessary?
                                         create_sample(datetimeformat) + ".000000\n",
                                         # TODO: Should this be necessary?
                                         create_sample(datetimeformat) +
                                         f":{SAMPLE_SECOND:02d}.000000\n"],
                              f"from datetimeformat {datetimeformat} and addl_hints {addl_hints}")
            except ModuleNotFoundError as e:
                if 'pandas' in str(e) and expect_pandas_failure:
                    # as expected
                    continue
                else:
                    raise
            self.assertFalse(expect_pandas_failure)

    def test_unload_datetimetz(self) -> None:
        self.datetime_fixture.createDateTimeTzTable()
        matching_dateformat = {
            'YYYY-MM-DD HH:MI:SS': 'YYYY-MM-DD',
            'YYYY-MM-DD HH12:MI AM': 'YYYY-MM-DD',
            'MM/DD/YY HH24:MI': 'MM/DD/YY',
            'YYYY-MM-DD HH24:MI:SSOF': 'YYYY-MM-DD',
            'YYYY-MM-DD HH:MI:SSOF': 'YYYY-MM-DD',
        }
        for datetimeformattz in DATETIMETZ_CASES:
            addl_hints: PartialRecordsHints = {}
            pandas_compatible_addl_hints: PartialRecordsHints = {
                'dateformat': matching_dateformat[datetimeformattz],
                'datetimeformat': datetimeformattz.replace('OF', ''),
            }
            uses_pandas = False
            if self.engine.name == 'redshift':
                if datetimeformattz != 'YYYY-MM-DD HH:MI:SSOF':
                    # this is the only format supported by Redshift on
                    # export
                    uses_pandas = True
            elif self.engine.name == 'mysql':
                # mysql has no exporter defined, so everything is via Pandas
                uses_pandas = True
            elif self.engine.name == 'postgresql':
                if datetimeformattz in ['YYYY-MM-DD HH:MI:SSOF',
                                        'YYYY-MM-DD HH24:MI:SSOF']:
                    # ensure we keep other areas of ISO style
                    addl_hints = {
                        'timeonlyformat': 'HH:MI:SS',
                        'dateformat': 'YYYY-MM-DD',
                        'datetimeformat': 'YYYY-MM-DD HH:MI:SS',
                    }
                else:
                    uses_pandas = True
            elif self.engine.name == 'vertica':
                # Make sure our '\n' strings below are valid when comparing output
                addl_hints['record-terminator'] = '\n'
                if datetimeformattz not in ['YYYY-MM-DD HH:MI:SSOF',
                                            'YYYY-MM-DD HH24:MI:SSOF']:
                    uses_pandas = True
            elif self.engine.name == 'bigquery':
                # All current export is via Avro
                uses_pandas = True
            if uses_pandas:
                # We're going to need to be sure to use hints that
                # work in Pandas
                addl_hints.update(pandas_compatible_addl_hints)
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
            expect_pandas_failure = not self.has_pandas() and uses_pandas
            try:
                csv_text = self.unload(column_name='timestamptz',
                                       records_format=records_format)
                self.assertIn(csv_text, [create_sample(datetimeformattz) + "\n",
                                         create_sample(datetimeformattz).
                                         replace('-00', '+00') + "\n",
                                         create_sample(datetimeformattz).replace('-00', '') +
                                         ".000000\n",
                                         # TODO: Should this be necessary?
                                         create_sample(datetimeformattz) + ".000000\n",
                                         # TODO: Should this be necessary?
                                         create_sample(datetimeformattz).replace('-00', '') +
                                         '.000000+0000\n',
                                         # TODO: Should this be necessary?
                                         create_sample(datetimeformattz) +
                                         f":{SAMPLE_SECOND:02d}.000000\n"
                                         ],
                              f"from datetimeformattz {datetimeformattz} and "
                              f"addl_hints {addl_hints}")
            except ModuleNotFoundError as e:
                if 'pandas' in str(e) and expect_pandas_failure:
                    # as expected
                    continue
                else:
                    raise
            self.assertFalse(expect_pandas_failure)

    def test_unload_timeonly(self) -> None:
        self.datetime_fixture.createTimeTable()
        for timeonlyformat in TIMEONLY_CASES:
            pandas_compatible_addl_hints: PartialRecordsHints = {
                'dateformat': 'YYYY-MM-DD',
                'datetimeformat': f'YYYY-MM-DD {timeonlyformat}',
                'datetimeformattz': f'YYYY-MM-DD {timeonlyformat}',
            }
            addl_hints: PartialRecordsHints = {}
            uses_pandas = False
            if self.engine.name == 'redshift' and not self.has_scratch_s3_bucket():
                uses_pandas = True
            elif self.engine.name == 'mysql':
                # mysql has no exporter defined, so everything is via Pandas
                uses_pandas = True
            elif self.engine.name == 'postgresql':
                if timeonlyformat in ['HH:MI:SS', 'HH24:MI:SS']:
                    # ensure we keep other areas of ISO style
                    addl_hints = {
                        'dateformat': 'YYYY-MM-DD',
                        'datetimeformat': 'YYYY-MM-DD HH:MI:SS',
                        'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
                    }
                else:
                    uses_pandas = True
            elif self.engine.name == 'vertica':
                # Make sure our '\n' strings below are valid when comparing output
                addl_hints['record-terminator'] = '\n'
                if timeonlyformat not in ['HH:MI:SS', 'HH24:MI:SS']:
                    uses_pandas = True
            elif self.engine.name == 'bigquery':
                # All current export is via Avro
                uses_pandas = True
            if uses_pandas:
                # we're going to need to be sure to use hints that work in Pandas
                addl_hints.update(pandas_compatible_addl_hints)
                if 'AM' in timeonlyformat:
                    # TODO: Add a GitHub issue for this
                    logger.warning('Cannot export this dateformat using Pandas'
                                   '--skipping test')
                    continue
            records_format = RecordsFormat(variant=VARIANT_FOR_DB[self.engine.name],
                                           hints={
                                               'timeonlyformat': timeonlyformat,
                                               'compression': None,
                                               'header-row': False,
                                               **addl_hints,  # type: ignore
                                           })
            expect_pandas_failure = not self.has_pandas() and uses_pandas
            try:
                csv_text = self.unload(column_name='time',
                                       records_format=records_format)
                allowed_items = [create_sample(timeonlyformat) + "\n",
                                 # create_sample(datetimeformattz).replace('-00', '+00') + "\n",
                                 # # TODO: Should this be necessary?
                                 # create_sample(datetimeformattz) + ".000000\n",
                                 # # TODO: Should this be necessary?
                                 # create_sample(datetimeformattz).replace('-00', '.000000+0000\n'),
                                 # # TODO: Should this be necessary?
                                 # create_sample(datetimeformattz) +
                                 # f":{SAMPLE_SECOND:02d}.000000\n"
                                 ]
                if self.engine.name == 'redshift':
                    # TODO point to issue here
                    allowed_items +=\
                        [f'{SAMPLE_HOUR:02d}:{SAMPLE_MINUTE:02d}:{SAMPLE_SECOND:02d}\n']
                self.assertIn(csv_text, allowed_items,
                              f"from timeonlyformat {timeonlyformat} and addl_hints {addl_hints}")
            except ModuleNotFoundError as e:
                if 'pandas' in str(e) and expect_pandas_failure:
                    # as expected
                    continue
                else:
                    raise
            self.assertFalse(expect_pandas_failure)
