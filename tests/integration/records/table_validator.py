import datetime
import logging
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text
from sqlalchemy.sql.elements import TextClause
from typing import Optional, Dict, Any, Union
from .timezone import set_session_tz
from .expected_column_types import (
    expected_single_database_column_types,
    expected_df_loaded_database_column_types,
    expected_table2table_column_types
)
from records_mover.records import DelimitedVariant
from .mover_test_case import MoverTestCase
from .table_timezone_validator import RecordsTableTimezoneValidator


logger = logging.getLogger(__name__)


#
# Terminology:
#
# unload_variant: Variant exported from the source database, or None
#                 if source database unloads via SELECT into a dataframe.
#
# file_variant: Variant of the file presented to records mover as the
#               source, or None if the move was from a database instead.
#
# load_variant: Variant which the target database will eventually load
#               from, or None if the database will be loaded via INSERT..
class RecordsTableValidator:
    def __init__(self,
                 target_db_engine: Engine,
                 source_db_engine: Optional[Engine] = None,
                 file_variant: Optional[DelimitedVariant] = None) -> None:
        """
        :param db_engine: Target database of the records move as a SQLAlchemy Engine object.
        :param source_db_engine: Source database of the records
        move.  None if we are loading from a file or a dataframe
        instead of copying from one database to another.
        :param target_db_engine: Target database of the records
        move.
        :param file_variant: None means the data was given to records mover via a Pandas
        dataframe or by copying from another database instead of a CSV.
        """
        self.target_db_engine = target_db_engine
        self.source_db_engine = source_db_engine
        self.file_variant = file_variant
        self.tc = MoverTestCase(target_db_engine=target_db_engine,
                                source_db_engine=source_db_engine,
                                file_variant=file_variant)
        self.tz_validator = RecordsTableTimezoneValidator(tc=self.tc,
                                                          target_db_engine=target_db_engine,
                                                          source_db_engine=source_db_engine,
                                                          file_variant=file_variant)

    def validate(self,
                 schema_name: str,
                 table_name: str) -> None:
        self.validate_data_types(schema_name, table_name)
        self.validate_data_values(schema_name, table_name)

    def validate_data_types(self, schema_name: str, table_name: str) -> None:
        if isinstance(self.target_db_engine, Engine):
            connection = self.target_db_engine.connect()
            columns = self.target_db_engine.dialect.get_columns(connection,
                                                                table_name,
                                                                schema=schema_name)
            connection.close()
        else:
            columns = self.target_db_engine.dialect.get_columns(self.target_db_engine,
                                                                table_name,
                                                                schema=schema_name)
        
        expected_column_names = [
            'num', 'numstr', 'str', 'comma', 'doublequote', 'quotecommaquote',
            'newlinestr', 'date', 'time', 'timestamp', 'timestamptz'
        ]
        actual_column_names = [column['name'] for column in columns]
        assert actual_column_names == expected_column_names, actual_column_names

        def format_type(column: Dict[str, Any]) -> str:
            suffix = ''
            if 'timezone' in column and column['timezone']:
                suffix = ' (tz)'
            return str(column['type']) + suffix

        actual_column_types = [format_type(column) for column in columns]

        def format_actual_expected_column_types(*expected):
            expected_str = '\nOR: '.join([str(x) for x in expected])
            return (f'\nACTUAL: {actual_column_types}\n'
                    f'EXPECTED: {expected_str}')

        expected_target_df_types = expected_df_loaded_database_column_types.get(
            self.target_db_engine.name)
        expected_target_single_types = \
            expected_single_database_column_types.get(
                self.target_db_engine.name)

        if self.source_db_engine is None:
            if self.file_variant is None:
                actual_expected_str = format_actual_expected_column_types(
                    expected_target_df_types,
                    expected_target_single_types)

                assert \
                    actual_column_types in (
                        expected_target_df_types,
                        expected_target_single_types), \
                    f'Could not find column types filed under ' \
                    f"{('df', self.target_db_engine.name)} or : " \
                    f'{self.target_db_engine.name}: ' \
                    f'{actual_expected_str}'
            else:
                actual_expected_str = format_actual_expected_column_types(
                    expected_target_single_types)

                assert actual_column_types == expected_target_single_types, \
                    f'Could not find column types filed under ' \
                    f'{self.target_db_engine.name}: ' \
                    f'{actual_expected_str}'
        else:
            assert (actual_column_types in
                    (expected_table2table_column_types.get((self.source_db_engine.name,
                                                            self.target_db_engine.name)),
                     expected_single_database_column_types[self.source_db_engine.name],
                     expected_single_database_column_types[self.target_db_engine.name],
                     expected_df_loaded_database_column_types.get(self.target_db_engine.name))),\
                f'Could not find column types filed under '\
                f"{(self.source_db_engine.name, self.target_db_engine.name)} "\
                'or either individually: '\
                f'{actual_column_types}'

    def validate_data_values(self,
                             schema_name: str,
                             table_name: str) -> None:
        params = {}

        load_variant = self.tc.determine_load_variant()

        with self.target_db_engine.connect() as connection:
            set_session_tz(connection)

            select_sql: Union[TextClause, str]
            if self.target_db_engine.name == 'bigquery':
                #
                # According to Google, "DATETIME is not supported for
                # uploading from Parquet" -
                # https://github.com/googleapis/google-cloud-python/issues/9996#issuecomment-572273407
                #
                # As a result, we upgrade DATETIME (no tz) columns to
                # TIMESTAMP (with timestamp) when loading with
                # Parquet.
                #
                # To be able to test both cases (parquet load vs. csv
                # load) with the same code, we go ahead and drop the
                # timezone on this column in this test validation code
                # for uniformity with a CAST().
                #
                # Similarly, when moving from MySQL, which doesn't
                # support a usable datetimetz type, we'll end up
                # creating a datetime type for the 'timestamptz'
                # column, and will need to cast.
                select_sql = text(f"""
                SELECT num, numstr, comma, doublequote, quotecommaquote, date, `time`,
                       CAST(`timestamp` AS datetime) as `timestamp`,
                       format_datetime(:formatstr, CAST(`timestamp` as datetime)) as timestampstr,
                       timestamptz,
                       format_timestamp(:tzformatstr, CAST(`timestamptz` as timestamp))
                       as timestamptzstr
                FROM {schema_name}.{table_name}
                """)
                params = {
                    "tzformatstr": "%E4Y-%m-%d %H:%M:%E*S %Z",
                    "formatstr": "%E4Y-%m-%d %H:%M:%E*S",
                }
            elif self.target_db_engine.name == 'mysql':
                select_sql = f"""
                SELECT num, numstr, comma, doublequote, quotecommaquote, date, `time`,
                       `timestamp`,
                       DATE_FORMAT(`timestamp`, '%%Y-%%m-%%d %%H:%%i:%%s.%%f') as timestampstr,
                       timestamptz,
                       DATE_FORMAT(timestamptz, '%%Y-%%m-%%d %%H:%%i:%%s.%%f+00') as timestamptzstr
                FROM {schema_name}.{table_name}
                """
            elif self.tc.raw_avro_types_written():
                # no real date/time column types used, so can't cast types
                select_sql = f"""
                SELECT num, numstr, comma, doublequote, quotecommaquote, date, "time",
                       "timestamp",
                       "timestamp" as timestampstr,
                       timestamptz,
                       timestamptz as timestamptzstr
                FROM {schema_name}.{table_name}
                """
            else:
                select_sql = f"""
                SELECT num, numstr, comma, doublequote, quotecommaquote, date, "time",
                       "timestamp",
                       to_char("timestamp", 'YYYY-MM-DD HH24:MI:SS.US') as timestampstr,
                       timestamptz, to_char(timestamptz,
                                            'YYYY-MM-DD HH24:MI:SS.US TZ') as timestamptzstr
                FROM {schema_name}.{table_name}
                """
            out = connection.execute(select_sql, **params)
            ret_all = out.fetchall()
        assert 1 == len(ret_all)
        ret = ret_all[0]

        assert ret['num'] == 123
        assert ret['numstr'] == '123', ret['numstr']
        assert ret['comma'] == ','
        assert ret['doublequote'] == '"'
        assert ret['quotecommaquote'] == '","'
        if self.tc.raw_avro_types_written():
            assert ret['date'] == 10957, ret['date']
        else:
            assert ret['date'] == datetime.date(2000, 1, 1),\
                f"Expected datetime.date(2000, 1, 1), got {ret['date']}"

        if self.tc.raw_avro_types_written():
            assert ret['time'] == 0, ret['time']
        elif self.tc.supports_time_without_date():
            if self.tc.selects_time_types_as_timedelta():
                assert ret['time'] == datetime.timedelta(0, 0),\
                    f"Incorrect time: {ret['time']} (of type {type(ret['time'])})"
            else:
                assert ret['time'] == datetime.time(0, 0),\
                    f"Incorrect time: {ret['time']} (of type {type(ret['time'])})"
        else:
            # fall back to storing as string
            if load_variant is not None and self.tc.variant_uses_am_pm(load_variant):
                assert ret['time'] == '12:00 AM', f"time was {ret['time']}"
            else:
                assert ret['time'] == '00:00:00', f"time was {ret['time']}"

        if self.tc.raw_avro_types_written():
            assert ret['timestamp'] == '2000-01-02T12:34:56.789012'
        elif (((load_variant is not None) and
               self.tc.variant_doesnt_support_seconds(load_variant)) or
              ((self.file_variant is not None) and
              self.tc.variant_doesnt_support_seconds(self.file_variant))):
            assert ret['timestamp'] ==\
                datetime.datetime(2000, 1, 2, 12, 34),\
                f"Found timestamp {ret['timestamp']}"

        else:
            assert (ret['timestamp'] == datetime.datetime(2000, 1, 2, 12, 34, 56, 789012)),\
                f"ret['timestamp'] was {ret['timestamp']} of type {type(ret['timestamp'])}"

        self.tz_validator.validate(timestampstr=ret['timestampstr'],
                                   timestamptzstr=ret['timestamptzstr'],
                                   timestamptz=ret['timestamptz'])
