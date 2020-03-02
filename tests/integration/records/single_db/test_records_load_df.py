import pytz
import logging
from .base_records_test import BaseRecordsIntegrationTest
import datetime
from odictliteral import odict
from ..table_validator import RecordsTableValidator
from records_mover.records import move


logger = logging.getLogger(__name__)


class RecordsLoadDataframeIntegrationTest(BaseRecordsIntegrationTest):
    def load_and_verify(self):
        if not self.has_pandas():
            logger.warning("Skipping test as we don't have Pandas to save with.")
            return

        from pandas import DataFrame

        us_eastern = pytz.timezone('US/Eastern')

        df = DataFrame.from_dict([odict[
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
        ]])

        with self.records.sources.dataframe(df=df) as source,\
            self.records.targets.table(schema_name=self.schema_name,
                                       table_name=self.table_name,
                                       db_engine=self.engine) as target:
            out = self.records.move(source, target)
            self.verify_db_table()
            return out

    def verify_db_table(self):
        validator = RecordsTableValidator(self.engine)
        validator.validate(None, schema_name=self.schema_name, table_name=self.table_name)

    def test_load_with_defaults(self):
        self.load_and_verify()
