import pytz
import datetime
import logging
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text
from sqlalchemy.sql.elements import TextClause
from typing import Optional, Dict, Any, Union
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
    def __init__(self, db_engine: Engine,
                 source_data_db_engine: Optional[Engine] = None,
                 file_variant: Optional[DelimitedVariant] = None) -> None:
        """
        :param db_engine: Target database of the records move.

        :param source_data_db_engine: Source database of the records
        move.  None if we are loading from a file or a dataframe
        instead of copying from one database to another.

        :param file_variant: None means the data was given to records mover via a Pandas
        dataframe or by copying from another database instead of a CSV.
        """
        self.engine = db_engine
        self.source_data_db_engine = source_data_db_engine
        self.file_variant = file_variant

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

    def supports_time_without_date(self) -> bool:
        # Redshift as a source or destination doesn't support a time
        # type, meaning the net result will be time as a string type.
        return (self.engine.name != 'redshift'
                and (self.source_data_db_engine is None or
                     self.source_data_db_engine.name != 'redshift'))

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
        columns = self.engine.dialect.get_columns(self.engine, table_name, schema=schema_name)
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

    def default_load_variant(self, db_engine: Engine) -> DelimitedVariant:
        if db_engine.name == 'bigquery':
            return 'bigquery'
        elif db_engine.name == 'vertica':
            return 'vertica'
        else:
            return 'bluelabs'

    def determine_load_variant(self) -> DelimitedVariant:
        if self.file_variant is None:

            # If we're not loading from a file, we're copying from a database
            if self.source_data_db_engine is None:
                # Loading from a dataframe
                return self.default_load_variant(self.engine)
            else:
                # Loading from a database
                if self.source_data_db_engine.name == 'bigquery':
                    return 'bigquery'
                else:
                    return 'vertica'
        else:
            return self.file_variant

    def loaded_from_dataframe(self) -> bool:
        return self.file_variant is None and self.source_data_db_engine is None

    def validate_data_values(self,
                             schema_name: str,
                             table_name: str) -> None:
        params = {}

        load_variant = self.determine_load_variant()
        with self.engine.connect() as connection:
            set_session_tz(connection)

            select_sql: Union[TextClause, str]
            if self.engine.name == 'bigquery':
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
                select_sql = text(f"""
                SELECT num, numstr, comma, doublequote, quotecommaquote, date, `time`,
                       CAST(`timestamp` AS datetime) as `timestamp`,
                       format_datetime(:formatstr, CAST(`timestamp` as datetime)) as timestampstr,
                       timestamptz,
                       format_timestamp(:tzformatstr, timestamptz) as timestamptzstr
                FROM {schema_name}.{table_name}
                """)
                params = {
                    "tzformatstr": "%E4Y-%m-%d %H:%M:%E*S %Z",
                    "formatstr": "%E4Y-%m-%d %H:%M:%E*S",
                }
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
            assert ret['time'] == datetime.time(0, 0), f"Incorrect time: {ret['time']}"
        else:
            # fall back to storing as string
            if self.variant_uses_am_pm(load_variant):
                assert ret['time'] == '12:00 AM', f"time was {ret['time']}"
            else:
                assert ret['time'] == '00:00:00', f"time was {ret['time']}"
        if self.variant_doesnt_support_seconds(load_variant):
            assert ret['timestamp'] ==\
                datetime.datetime(2000, 1, 2, 12, 34),\
                f"Found timestamp {ret['timestamp']}"

        else:
            assert (ret['timestamp'] == datetime.datetime(2000, 1, 2, 12, 34, 56, 789012)),\
                f"ret['timestamp'] was {ret['timestamp']}"

        if (self.loaded_from_dataframe() and
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
        if self.variant_doesnt_support_seconds(load_variant):
            seconds = '00'
            micros = '000000'
        else:
            seconds = '56'
            micros = '789012'

        assert ret['timestampstr'] == f'2000-01-02 12:34:{seconds}.{micros}', ret['timestampstr']

        assert ret['timestamptzstr'] in [
                f'2000-01-02 {utc_hour}:34:{seconds}.{micros} UTC',
                f'2000-01-02 {utc_hour}:34:{seconds}.{micros}+00'
            ],\
            (f"translated ret['timestamptzstr'] was {ret['timestamptzstr']} and "
             f"class is {type(ret['timestamptzstr'])} - expected "
             f"hour to be {utc_hour}")

        utc = pytz.timezone('UTC')
        if self.variant_doesnt_support_seconds(load_variant):
            utc_naive_expected_time = datetime.datetime(2000, 1, 2, utc_hour, 34)
        else:
            utc_naive_expected_time = datetime.datetime(2000, 1, 2, utc_hour, 34, 56, 789012)
        utc_expected_time = utc.localize(utc_naive_expected_time)

        # Dunno why sqlalchemy doesn't return this instead, but
        # timestamptzstr shows that db knows what's up internally:
        #
        actual_time = ret['timestamptz']
        assert actual_time - utc_expected_time == datetime.timedelta(0),\
            f"Delta is {actual_time - utc_expected_time}, " \
            f"actual_time is {actual_time}, expected time is {utc_expected_time}"
