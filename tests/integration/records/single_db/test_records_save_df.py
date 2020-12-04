import pytz
import logging
from .base_records_test import BaseRecordsIntegrationTest
from ..directory_validator import RecordsDirectoryValidator
from records_mover.records import (
    RecordsSchema, DelimitedRecordsFormat, ProcessingInstructions
)
import tempfile
import pathlib
import datetime


logger = logging.getLogger(__name__)


class RecordsSaveDataframeIntegrationTest(BaseRecordsIntegrationTest):
    def save_and_verify(self, records_format, processing_instructions=None) -> None:
        if not self.has_pandas():
            logger.warning("Skipping test as we don't have Pandas to save with.")
            return

        from pandas import DataFrame

        if processing_instructions is None:
            processing_instructions = ProcessingInstructions()
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

        records_schema = RecordsSchema.from_dataframe(df,
                                                      processing_instructions,
                                                      include_index=False)
        records_schema = records_schema.refine_from_dataframe(df, processing_instructions)

        with tempfile.TemporaryDirectory(prefix='test_records_save_df') as tempdir:
            output_url = pathlib.Path(tempdir).resolve().as_uri() + '/'
            source = self.records.sources.dataframe(df=df,
                                                    records_schema=records_schema,
                                                    processing_instructions=processing_instructions)
            target = self.records.targets.directory_from_url(output_url,
                                                             records_format=records_format)
            out = self.records.move(source, target, processing_instructions)
            self.verify_records_directory(records_format.format_type,
                                          records_format.variant,
                                          tempdir,
                                          records_format.hints)
            return out

    def verify_records_directory(self, format_type, variant, tempdir, hints={}) -> None:
        validator = RecordsDirectoryValidator(tempdir,
                                              self.resource_name(format_type, variant, hints),
                                              self.engine.name)
        validator.validate()

    def test_save_with_defaults(self):
        hints = {}
        self.save_and_verify(records_format=DelimitedRecordsFormat(hints=hints))

    def test_save_with_defaults_dd_dash_mm(self):
        hints = {
            'datetimeformattz': 'DD-MM-YY HH24:MIOF',
            'datetimeformat': 'DD-MM-YY HH24:MI',
            'dateformat': 'DD-MM-YY'
        }
        self.save_and_verify(records_format=DelimitedRecordsFormat(hints=hints))

    def test_save_csv_variant(self):
        records_format = DelimitedRecordsFormat(variant='csv')
        self.save_and_verify(records_format=records_format)

    def test_save_csv_variant_dd_dash_mm(self):
        hints = {
            'datetimeformattz': 'DD-MM-YY HH24:MIOF',
            'datetimeformat': 'DD-MM-YY HH24:MI',
            'dateformat': 'DD-MM-YY'
        }
        records_format = DelimitedRecordsFormat(variant='csv',
                                                hints=hints)
        self.save_and_verify(records_format=records_format)

    def test_save_with_no_compression(self):
        hints = {
            'compression': None,
        }
        records_format = DelimitedRecordsFormat(hints=hints)
        self.save_and_verify(records_format=records_format)
