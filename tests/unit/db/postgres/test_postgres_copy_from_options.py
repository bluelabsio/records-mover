import unittest
from records_mover.db.postgres.copy_options import postgres_copy_from_options
from records_mover.records import ProcessingInstructions, DelimitedRecordsFormat
from records_mover.records.load_plan import RecordsLoadPlan


class TestPostgresCopyFromOptions(unittest.TestCase):
    def test_new_compression_hint(self):
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={'compression': None})
        records_format.hints['encoding'] = 'NEWNEWENCODING'
        unhandled_hints = set()
        processing_instructions = ProcessingInstructions()
        load_plan = RecordsLoadPlan(processing_instructions,
                                    records_format)
        with self.assertRaises(NotImplementedError):
            postgres_copy_from_options(unhandled_hints, load_plan)

    def test_bluelabs_minus_escaping(self):
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={'compression': None})
        records_format.hints['escape'] = None
        unhandled_hints = set()
        processing_instructions = ProcessingInstructions()
        load_plan = RecordsLoadPlan(processing_instructions,
                                    records_format)
        with self.assertRaises(NotImplementedError):
            postgres_copy_from_options(unhandled_hints, load_plan)

    def test_bluelabs_with_doublequoting(self):
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={'compression': None})
        records_format.hints['doublequote'] = '"'
        unhandled_hints = set()
        processing_instructions = ProcessingInstructions()
        load_plan = RecordsLoadPlan(processing_instructions,
                                    records_format)
        with self.assertRaises(NotImplementedError):
            postgres_copy_from_options(unhandled_hints, load_plan)

    def test_vertica(self):
        records_format = DelimitedRecordsFormat(variant='vertica',
                                                hints={'compression': None})
        records_format.hints['escape'] = '\\'
        unhandled_hints = set()
        processing_instructions = ProcessingInstructions()
        load_plan = RecordsLoadPlan(processing_instructions,
                                    records_format)
        with self.assertRaises(NotImplementedError):
            postgres_copy_from_options(unhandled_hints, load_plan)

    def test_bluelabs_with_compression(self):
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={'compression': 'GZIP'})
        unhandled_hints = set()
        processing_instructions = ProcessingInstructions()
        load_plan = RecordsLoadPlan(processing_instructions,
                                    records_format)
        with self.assertRaises(NotImplementedError):
            postgres_copy_from_options(unhandled_hints, load_plan)

    def test_csv_with_escaping(self):
        records_format = DelimitedRecordsFormat(variant='csv',
                                                hints={'compression': None,
                                                       'escape': '\\'})
        unhandled_hints = set()
        processing_instructions = ProcessingInstructions()
        load_plan = RecordsLoadPlan(processing_instructions,
                                    records_format)
        with self.assertRaises(NotImplementedError):
            postgres_copy_from_options(unhandled_hints, load_plan)

    def test_csv_no_doublequote(self):
        records_format = DelimitedRecordsFormat(variant='csv',
                                                hints={'compression': None,
                                                       'doublequote': None})
        unhandled_hints = set()
        processing_instructions = ProcessingInstructions()
        load_plan = RecordsLoadPlan(processing_instructions,
                                    records_format)
        with self.assertRaises(NotImplementedError):
            postgres_copy_from_options(unhandled_hints, load_plan)

    def test_csv_quote_all(self):
        records_format = DelimitedRecordsFormat(variant='csv',
                                                hints={'compression': None,
                                                       'quoting': 'all'})
        unhandled_hints = set()
        processing_instructions = ProcessingInstructions()
        load_plan = RecordsLoadPlan(processing_instructions,
                                    records_format)
        with self.assertRaises(NotImplementedError):
            postgres_copy_from_options(unhandled_hints, load_plan)
