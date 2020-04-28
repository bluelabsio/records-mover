import unittest
from records_mover.records.pandas.read_csv_options import pandas_read_csv_options
from records_mover.records.schema import RecordsSchema
from records_mover.records import DelimitedRecordsFormat, ProcessingInstructions


class TestReadCsvOptions(unittest.TestCase):
    def test_pandas_read_csv_options_bzip(self):
        records_format = DelimitedRecordsFormat(hints={
            'compression': 'BZIP'
        })
        records_schema = RecordsSchema.from_data({
            'schema': 'bltypes/v1'
        })
        unhandled_hints = set()
        processing_instructions = ProcessingInstructions()
        expectations = {
            'compression': 'bz2'
        }
        out = pandas_read_csv_options(records_format,
                                      records_schema,
                                      unhandled_hints,
                                      processing_instructions)
        self.assertTrue(all(item in out.items() for item in expectations.items()))
