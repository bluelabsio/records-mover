import pytz
import datetime
import logging
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text
from sqlalchemy.sql.elements import TextClause
from typing import Optional, Dict, Any, Union, List
from .timezone import set_session_tz
from .expected_column_types import expected_column_types
from records_mover.records import DelimitedVariant


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
        :param db_engine: Target database of the records move.

        :param source_data_db_engine: Source database of the records
        move.  None if we are loading from a file or a dataframe
        instead of copying from one database to another.

        :param file_variant: None means the data was given to records mover via a Pandas
        dataframe or by copying from another database instead of a CSV.
        """
        self.target_db_engine = target_db_engine
        self.source_db_engine = source_db_engine
        self.file_variant = file_variant

    def database_has_no_usable_timestamptz_type(self, engine: Engine) -> bool:
        # If true, timestamptzs are treated like timestamps, and the
        # timezone is stripped off without looking at it before the
        # timestamp itself is stored without any modifications to the
        # hour number.
        return engine.name == 'mysql'

    def database_default_store_timezone_is_us_eastern(self) -> bool:
        """
        If we don't specify a timezone in a timestamptz string, does the
        database assign the US/Eastern timezone when it's stored?
        """

        # We've seen this for some Vertica servers in the past, but it
        # doesn't affect our current integration test targets.

        # This seems to be controlled in Vertica by what timezone is
        # set on the cluster servers at Vertica install-time.  The
        # Docker image (jbfavre/vertica) uses UTC, but our physical
        # servers when integration tests are run by hand does not.
        return False

    def selects_time_types_as_timedelta(self):
        return self.target_db_engine.name == 'mysql'

    def supports_time_without_date(self) -> bool:
        # Redshift as a source or destination doesn't support a time
        # type, meaning the net result will be time as a string type.
        return (self.target_db_engine.name != 'redshift'
                and (self.source_db_engine is None or
                     self.source_db_engine.name != 'redshift'))

    def variant_doesnt_support_seconds(self, variant: DelimitedVariant):
        # things are represented as second-denominated date + time
        #
        # e.g. - 1/1/00,12:00 AM
        return variant == 'csv'

    def variant_doesnt_support_timezones(self,
                                         variant: Optional[DelimitedVariant]) -> bool:
        return variant in ['csv', 'bigquery']

    def variant_uses_am_pm(self, variant: DelimitedVariant) -> bool:
        return variant == 'csv'

    def validate(self,
                 schema_name: str,
                 table_name: str) -> None:
        self.validate_data_types(schema_name, table_name)
        self.validate_data_values(schema_name, table_name)

    def validate_data_types(self, schema_name: str, table_name: str) -> None:
        columns = self.target_db_engine.dialect.get_columns(self.target_db_engine, table_name, schema=schema_name)
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
        assert actual_column_types in expected_column_types, actual_column_types

    def supported_load_variants(self, db_engine: Engine) -> List[DelimitedVariant]:
        if db_engine.name == 'bigquery':
            return ['bigquery']
        elif db_engine.name == 'vertica':
            return ['bluelabs', 'vertica']
        elif db_engine.name == 'redshift':
            # This isn't really true, but is good enough to make the
            # tests pass for now.  We need to create a new named
            # variant name for the CSV-esque variant that we now
            # prefer for Redshift.
            return ['bluelabs', 'csv', 'bigquery']
        elif db_engine.name == 'postgresql':
            return ['bluelabs', 'csv', 'bigquery']
        elif db_engine.name == 'mysql':
            return []
        else:
            raise NotImplementedError(f"Teach me about database type {db_engine.name}")

    def default_load_variant(self, db_engine: Engine) -> Optional[DelimitedVariant]:
        supported = self.supported_load_variants(db_engine)
        if len(supported) == 0:
            return None
        return supported[0]

    def determine_load_variant(self) -> Optional[DelimitedVariant]:
        if self.loaded_from_file():
            if self.file_variant in self.supported_load_variants(self.target_db_engine):
                return self.file_variant
            else:
                return self.default_load_variant(self.target_db_engine)
        else:
            # If we're not loading from a file, we're copying from a database
            if self.loaded_from_dataframe():
                # Loading from a dataframe
                return self.default_load_variant(self.target_db_engine)
            else:
                # Loading from a database
                assert self.source_db_engine is not None
                if self.source_db_engine.name == 'bigquery':
                    return 'bigquery'
                else:
                    return 'vertica'

    def loaded_from_database(self) -> bool:
        return self.source_db_engine is not None

    def loaded_from_dataframe(self) -> bool:
        return self.file_variant is None and self.source_db_engine is None

    def loaded_from_file(self) -> bool:
        return self.file_variant is not None

    def validate_data_values(self,
                             schema_name: str,
                             table_name: str) -> None:
        params = {}

        load_variant = self.determine_load_variant()
        print(f"VMB: load_variant: {load_variant}")

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
                       format_timestamp(:tzformatstr, CAST(`timestamptz` as timestamp)) as timestamptzstr
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
        assert ret['numstr'] == '123'
        assert ret['comma'] == ','
        assert ret['doublequote'] == '"'
        assert ret['quotecommaquote'] == '","'
        assert ret['date'] == datetime.date(2000, 1, 1),\
            f"Expected datetime.date(2000, 1, 1), got {ret['date']}"

        if self.supports_time_without_date():
            if self.selects_time_types_as_timedelta():
                assert ret['time'] == datetime.timedelta(0, 0),\
                    f"Incorrect time: {ret['time']} (of type {type(ret['time'])})"
            else:
                assert ret['time'] == datetime.time(0, 0),\
                    f"Incorrect time: {ret['time']} (of type {type(ret['time'])})"
        else:
            # fall back to storing as string
            if load_variant is not None and self.variant_uses_am_pm(load_variant):
                assert ret['time'] == '12:00 AM', f"time was {ret['time']}"
            else:
                assert ret['time'] == '00:00:00', f"time was {ret['time']}"
        if (((load_variant is not None) and
             self.variant_doesnt_support_seconds(load_variant)) or
           ((self.file_variant is not None) and
           self.variant_doesnt_support_seconds(self.file_variant))):
            assert ret['timestamp'] ==\
                datetime.datetime(2000, 1, 2, 12, 34),\
                f"Found timestamp {ret['timestamp']}"

        else:
            assert (ret['timestamp'] == datetime.datetime(2000, 1, 2, 12, 34, 56, 789012)),\
                f"ret['timestamp'] was {ret['timestamp']} of type {type(ret['timestamp'])}"

        if (self.source_db_engine is not None and
           self.database_has_no_usable_timestamptz_type(self.source_db_engine)):
            # The source database doesn't know anything about
            # timezones, and records_database_fixture.py inserts
            # something like "2000-01-02 12:34:56.789012-05" - and the
            # timezone parts gets ignored by the database.
            #
            utc_hour = 12
        elif (self.loaded_from_file() and
              self.database_has_no_usable_timestamptz_type(self.target_db_engine)):
            # In this case, we're trying to load a string that looks like this:
            #
            #  2000-01-02 12:34:56.789012-05
            #
            # But since we're loading it into a column type that
            # doesn't store timezones, the database in question just
            # strips off the timezone and stores the '12'
            utc_hour = 12
        elif(self.loaded_from_dataframe() and
             self.database_has_no_usable_timestamptz_type(self.target_db_engine)):
            #
            # In this case, we correctly tell Pandas that we have are
            # at noon:34 US/Eastern, and tell Pandas to format the
            # datetime format.
            #
            # But since we're loading it into a column type that
            # doesn't store timezones, the database in question just
            # strips off the timezone and stores the '12'
            #
            utc_hour = 12
        elif (self.loaded_from_file() and
              load_variant != self.file_variant and
              self.variant_doesnt_support_timezones(load_variant) and
              not self.database_default_store_timezone_is_us_eastern()):
            # In this case, we're trying to load a string that looks like this:
            #
            #  2000-01-02 12:34:56.789012-05
            #
            # That gets correct turned into a dataframe representing
            # noon:34 US/Eastern.  We tell Pandas to format the
            # datetime format in the load variant.  Unfortunately, if
            # you don't specify a timezone as part of that format,
            # Pandas just prints the TZ-naive hour.
            utc_hour = 12
        elif (self.loaded_from_dataframe() and
              self.variant_doesnt_support_timezones(load_variant) and
              not self.database_default_store_timezone_is_us_eastern()):
            #
            # In this case, we correctly tell Pandas that we have are
            # at noon:34 US/Eastern, and tell Pandas to format the
            # datetime format.  Unfortunately, if you don't specify a
            # timezone as part of that format, Pandas just prints the
            # TZ-naive hour.
            #
            utc_hour = 12
        elif (self.variant_doesnt_support_timezones(self.file_variant) and
              not self.database_default_store_timezone_is_us_eastern()):
            # In this case we're loading from one of our example
            # files, but the example file doesn't contain a timezone.
            # Per tests/integration/resources/README.md:
            #
            #     On systems where a timezone can't be represented,
            #     this should be represented as if the implicit
            #     timezone was US/Eastern.
            #
            # Example date that we'd be loading as a string:
            #
            #   2000-01-02 12:34:56.789012
            #
            # Per our tests/integration/resources/README.md, this is
            # representing noon:34 on the east coast.
            #
            # However, if we load into a database who assumes that
            # timezoneless times coming in are in in UTC, when we
            # select it back out in UTC form, it'll come back as noon
            # UTC!
            utc_hour = 12
        else:
            # ...if, however, either the variant *does* support
            # timezones, if it gets rendered back as UTC, it'll be at
            # hour 17 UTC - which is noon US/Eastern.

            # ...and if the database assumes US/Eastern when storing,
            # the same result will happen, as the database will
            # understand that noon on the east coast is hour 17 UTC.
            utc_hour = 17
        if ((load_variant is not None and
             self.variant_doesnt_support_seconds(load_variant)) or
            ((self.file_variant is not None and
              self.variant_doesnt_support_seconds(self.file_variant)))):
            seconds = '00'
            micros = '000000'
        else:
            seconds = '56'
            micros = '789012'

        assert ret['timestampstr'] == f'2000-01-02 12:34:{seconds}.{micros}',\
            f"expected '2000-01-02 12:34:{seconds}.{micros}' got '{ret['timestampstr']}'"

        if (self.source_db_engine is not None and
           self.database_has_no_usable_timestamptz_type(self.source_db_engine)):
            # Depending on the capabilities of the target database, we
            # may not be able to get a rendered version that includes
            # the UTC tz - but either way we won't have transferred a
            # timezone in.
            assert ret['timestamptzstr'] in [
                f'2000-01-02 {utc_hour}:34:{seconds}.{micros} ',
                f'2000-01-02 {utc_hour}:34:{seconds}.{micros} UTC',
            ],\
                (f"translated ret['timestamptzstr'] was {ret['timestamptzstr']} and "
                 f"class is {type(ret['timestamptzstr'])} - expected "
                 f"hour to be {utc_hour}")
        else:
            assert ret['timestamptzstr'] in [
                f'2000-01-02 {utc_hour}:34:{seconds}.{micros} UTC',
                f'2000-01-02 {utc_hour}:34:{seconds}.{micros}+00'
            ],\
                (f"translated ret['timestamptzstr'] was {ret['timestamptzstr']} and "
                 f"class is {type(ret['timestamptzstr'])} - expected "
                 f"hour to be {utc_hour}")

        utc = pytz.timezone('UTC')
        if ((load_variant is not None and
             self.variant_doesnt_support_seconds(load_variant)) or
            (self.file_variant is not None and
             self.variant_doesnt_support_seconds(self.file_variant))):
            utc_naive_expected_time = datetime.datetime(2000, 1, 2, utc_hour, 34)
        else:
            utc_naive_expected_time = datetime.datetime(2000, 1, 2, utc_hour, 34, 56, 789012)
        utc_expected_time = utc.localize(utc_naive_expected_time)

        # Dunno why sqlalchemy doesn't return this instead, but
        # timestamptzstr shows that db knows what's up internally:
        #
        actual_time = ret['timestamptz']
        if actual_time.tzinfo is None:
            assert actual_time - utc_naive_expected_time == datetime.timedelta(0),\
                f"Delta is {actual_time - utc_naive_expected_time}, " \
                f"actual_time is {actual_time}, tz-naive expected time is {utc_naive_expected_time}"
        else:
            assert actual_time - utc_expected_time == datetime.timedelta(0),\
                f"Delta is {actual_time - utc_expected_time}, " \
                f"actual_time is {actual_time}, expected time is {utc_expected_time}"
