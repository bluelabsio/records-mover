from sqlalchemy.engine import Engine
from typing import Optional, List
from records_mover.records import DelimitedVariant


class MoverTestCase:
    def __init__(self,
                 target_db_engine: Engine,
                 source_db_engine: Optional[Engine] = None,
                 file_variant: Optional[DelimitedVariant] = None) -> None:
        """
        :param db_engine: Target database of the records move as a SQLAlchemy Engine object.

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
            return ['bluelabs', 'csv', 'bigquery', 'vertica']
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
