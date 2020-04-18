from sqlalchemy.engine import Engine
from typing import Optional
from records_mover.records import DelimitedVariant


class MoverTestCase:
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

    def selects_time_types_as_timedelta(self) -> bool:
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
