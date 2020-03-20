import unittest
from records_mover.db.postgres.loader import PostgresLoader
from records_mover.db.postgres.copy_options import postgres_copy_options
from records_mover.records import ProcessingInstructions, DelimitedRecordsFormat
from records_mover.records.load_plan import RecordsLoadPlan
from mock import Mock


class TestPostgresCopyOptionsLoadKnown(unittest.TestCase):
    def test_load_known_formats(self):
        mock_url_resolver = Mock(name='url_resolver')
        mock_meta = Mock(name='meta')
        mock_db = Mock(name='db')
        loader = PostgresLoader(url_resolver=mock_url_resolver,
                                meta=mock_meta,
                                db=mock_db)
        known_load_formats = loader.known_supported_records_formats_for_load()
        for records_format in known_load_formats:
            unhandled_hints = set()
            processing_instructions = ProcessingInstructions()
            load_plan = RecordsLoadPlan(processing_instructions,
                                        records_format)
            # ensure no exception thrown
            postgres_copy_options(unhandled_hints,
                                  load_plan)

    def test_bluelabs_uncompressed(self):
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={'compression': None})
        unhandled_hints = set()
        processing_instructions = ProcessingInstructions()
        load_plan = RecordsLoadPlan(processing_instructions,
                                    records_format)
        date_input_style, copy_options = postgres_copy_options(unhandled_hints,
                                                               load_plan)
        self.assertEqual(date_input_style, None)
        self.assertEqual(copy_options, {
            'format': 'text',
            'encoding': 'UTF8',
            'header': False,
            'delimiter': ','
        })

    def test_csv_uncompressed(self):
        records_format = DelimitedRecordsFormat(variant='csv',
                                                hints={'compression': None})
        unhandled_hints = set()
        processing_instructions = ProcessingInstructions()
        load_plan = RecordsLoadPlan(processing_instructions,
                                    records_format)
        date_input_style, copy_options = postgres_copy_options(unhandled_hints,
                                                               load_plan)
        self.assertEqual(date_input_style, 'MDY')
        self.assertEqual(copy_options, {
            'format': 'csv',
            'quote': '"',
            'encoding': 'UTF8',
            'header': True,
            'delimiter': ','
        })

    def test_bigquery_uncompressed(self):
        records_format = DelimitedRecordsFormat(variant='bigquery',
                                                hints={'compression': None})
        unhandled_hints = set()
        processing_instructions = ProcessingInstructions()
        load_plan = RecordsLoadPlan(processing_instructions,
                                    records_format)
        date_input_style, copy_options = postgres_copy_options(unhandled_hints,
                                                               load_plan)
        self.assertEqual(date_input_style, None)
        self.assertEqual(copy_options, {
            'format': 'csv',
            'quote': '"',
            'encoding': 'UTF8',
            'header': True,
            'delimiter': ','
        })
