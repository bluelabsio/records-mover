import pytz
import logging
from .base_records_test import BaseRecordsIntegrationTest
import datetime
from ..table_validator import RecordsTableValidator


logger = logging.getLogger(__name__)


class RecordsLoadDataframeIntegrationTest(BaseRecordsIntegrationTest):
    def load_and_verify(self) -> None:
        redshift_with_no_bucket = self.engine.name == 'redshift' and not self.has_scratch_bucket()
        if redshift_with_no_bucket:
            # https://github.com/bluelabsio/records-mover/issues/81
            logger.warning("This test won't pass until we can use the "
                           "records schema from the target to cast the "
                           "dataframe types appropriately, so skipping.")
            return

        if not self.has_pandas():
            logger.warning("Skipping test as we don't have Pandas to save with.")
            return

        from pandas import DataFrame

        us_eastern = pytz.timezone('US/Eastern')

        df = DataFrame.from_dict([{
            'num': 123,
            'numstr': '123',
            'str': 'foo',
            'comma': ',',
            'doublequote': '"',
            'quotecommaquote': '","',
            'newlinestr': ("* SQL unload would generate multiple files (one for each slice/part)\n"
                           "* Filecat would produce a single data file"),
            'date': datetime.date(2000, 1, 1),
            'time': datetime.time(0, 0),
            'timestamp': datetime.datetime(2000, 1, 2, 12, 34, 56, 789012),
            'timestamptz': us_eastern.localize(datetime.datetime(2000, 1, 2, 12, 34, 56, 789012))
        }])

        source = self.records.sources.dataframe(df=df)
        target = self.records.targets.table(schema_name=self.schema_name,
                                            table_name=self.table_name,
                                            db_engine=self.engine)
        out = self.records.move(source, target)
        self.verify_db_table()
        return out

    def verify_db_table(self) -> None:
        validator = RecordsTableValidator(self.engine)
        validator.validate(schema_name=self.schema_name, table_name=self.table_name)

    def test_load_with_defaults(self):
        self.load_and_verify()
