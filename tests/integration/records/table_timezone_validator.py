import pytz
import datetime
from sqlalchemy.engine import Engine
from typing import Optional
from records_mover.records import DelimitedVariant
from .mover_test_case import MoverTestCase


class RecordsTableTimezoneValidator:
    def __init__(self,
                 tc: MoverTestCase,
                 target_db_engine: Engine,
                 source_db_engine: Optional[Engine] = None,
                 file_variant: Optional[DelimitedVariant] = None) -> None:
        self.tc = tc
        self.target_db_engine = target_db_engine
        self.source_db_engine = source_db_engine
        self.file_variant = file_variant

    def validate(self,
                 timestampstr: str,
                 timestamptzstr: str,
                 timestamptz: datetime.datetime) -> None:
        load_variant = self.tc.determine_load_variant()

        if (self.source_db_engine is not None and
           self.tc.database_has_no_usable_timestamptz_type(self.source_db_engine)):
            # The source database doesn't know anything about
            # timezones, and records_database_fixture.py inserts
            # something like "2000-01-02 12:34:56.789012-05" - and the
            # timezone parts gets ignored by the database.
            #
            utc_hour = 12
        elif (self.tc.loaded_from_file() and
              self.tc.database_has_no_usable_timestamptz_type(self.target_db_engine)):
            # In this case, we're trying to load a string that looks like this:
            #
            #  2000-01-02 12:34:56.789012-05
            #
            # But since we're loading it into a column type that
            # doesn't store timezones, the database in question just
            # strips off the timezone and stores the '12'
            utc_hour = 12
        elif(self.tc.loaded_from_dataframe() and
             self.tc.database_has_no_usable_timestamptz_type(self.target_db_engine)):
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
        elif (self.tc.loaded_from_file() and
              load_variant != self.file_variant and
              self.tc.variant_doesnt_support_timezones(load_variant) and
              not self.tc.database_default_store_timezone_is_us_eastern()):
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
        elif (self.tc.loaded_from_dataframe() and
              self.tc.variant_doesnt_support_timezones(load_variant) and
              not self.tc.database_default_store_timezone_is_us_eastern()):
            #
            # In this case, we correctly tell Pandas that we have are
            # at noon:34 US/Eastern, and tell Pandas to format the
            # datetime format.  Unfortunately, if you don't specify a
            # timezone as part of that format, Pandas just prints the
            # TZ-naive hour.
            #
            utc_hour = 12
        elif (self.tc.variant_doesnt_support_timezones(self.file_variant) and
              not self.tc.database_default_store_timezone_is_us_eastern()):
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
             self.tc.variant_doesnt_support_seconds(load_variant)) or
            ((self.file_variant is not None and
              self.tc.variant_doesnt_support_seconds(self.file_variant)))):
            seconds = '00'
            micros = '000000'

            # Some databases on rendering this as a string will just
            # drop the microseconds if they're all zeros.
            assert timestampstr in [f'2000-01-02 12:34:{seconds}.{micros}',
                                    f'2000-01-02 12:34:{seconds}'],\
                f"expected '2000-01-02 12:34:{seconds}.{micros}' got '{timestampstr}'"

        else:
            seconds = '56'
            micros = '789012'

            assert timestampstr == f'2000-01-02 12:34:{seconds}.{micros}',\
                f"expected '2000-01-02 12:34:{seconds}.{micros}' got '{timestampstr}'"

        if (self.source_db_engine is not None and
           self.tc.database_has_no_usable_timestamptz_type(self.source_db_engine)):
            # Depending on the capabilities of the target database, we
            # may not be able to get a rendered version that includes
            # the UTC tz - but either way we won't have transferred a
            # timezone in.
            assert timestamptzstr in [
                f'2000-01-02 {utc_hour}:34:{seconds}.{micros} ',
                f'2000-01-02 {utc_hour}:34:{seconds}.{micros} UTC',
                f'2000-01-02 {utc_hour}:34:{seconds}.{micros}+00'
            ],\
                (f"translated timestamptzstr was {timestamptzstr} and "
                 f"class is {type(timestamptzstr)} - expected "
                 f"hour to be {utc_hour}")
        else:
            if micros == '000000':
                assert timestamptzstr in [
                    f'2000-01-02 {utc_hour}:34:{seconds}.{micros} UTC',
                    f'2000-01-02 {utc_hour}:34:{seconds} UTC',
                    f'2000-01-02 {utc_hour}:34:{seconds}.{micros}+00'
                ],\
                    (f"translated timestamptzstr was {timestamptzstr} and "
                     f"class is {type(timestamptzstr)} - expected "
                     f"hour to be {utc_hour}")
            else:
                assert timestamptzstr in [
                    f'2000-01-02 {utc_hour}:34:{seconds}.{micros} UTC',
                    f'2000-01-02 {utc_hour}:34:{seconds}.{micros}+00'
                ],\
                    (f"translated timestamptzstr was {timestamptzstr} and "
                     f"class is {type(timestamptzstr)} - expected "
                     f"hour to be {utc_hour}")

        utc = pytz.timezone('UTC')
        if ((load_variant is not None and
             self.tc.variant_doesnt_support_seconds(load_variant)) or
            (self.file_variant is not None and
             self.tc.variant_doesnt_support_seconds(self.file_variant))):
            utc_naive_expected_time = datetime.datetime(2000, 1, 2, utc_hour, 34)
        else:
            utc_naive_expected_time = datetime.datetime(2000, 1, 2, utc_hour, 34, 56, 789012)
        utc_expected_time = utc.localize(utc_naive_expected_time)

        # Dunno why sqlalchemy doesn't return this instead, but
        # timestamptzstr shows that db knows what's up internally:
        #
        actual_time = timestamptz
        if actual_time.tzinfo is None:
            assert actual_time - utc_naive_expected_time == datetime.timedelta(0),\
                f"Delta is {actual_time - utc_naive_expected_time}, " \
                f"actual_time is {actual_time}, tz-naive expected time is {utc_naive_expected_time}"
        else:
            assert actual_time - utc_expected_time == datetime.timedelta(0),\
                f"Delta is {actual_time - utc_expected_time}, " \
                f"actual_time is {actual_time}, expected time is {utc_expected_time}"
