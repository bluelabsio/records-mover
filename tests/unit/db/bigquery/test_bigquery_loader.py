import unittest

from records_mover.db.bigquery.loader import BigQueryLoader
from records_mover.records.records_format import DelimitedRecordsFormat, ParquetRecordsFormat
from mock import MagicMock, Mock
from unittest.mock import patch


class TestBigQueryLoader(unittest.TestCase):
    @patch('records_mover.db.bigquery.loader.load_job_config')
    def test_load_with_bad_schema_name(self, mock_load_job_config):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        big_query_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver)
        mock_schema = 'my_project.my_dataset.something_invalid'
        mock_table = Mock(name='mock_table')
        mock_load_plan = Mock(name='mock_load_plan')
        mock_load_plan.records_format =\
            Mock(name='source_records_format', spec=DelimitedRecordsFormat)
        mock_target_records_format = mock_load_plan.records_format
        mock_target_records_format.hints = {}
        mock_directory = Mock(name='mock_directory')
        mock_url = Mock(name='mock_url')
        mock_directory.manifest_entry_urls.return_value = [mock_url]

        with self.assertRaises(ValueError):
            big_query_loader.load(schema=mock_schema, table=mock_table,
                                  load_plan=mock_load_plan,
                                  directory=mock_directory)

    @patch('records_mover.db.bigquery.loader.load_job_config')
    def test_load(self, mock_load_job_config):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        mock_file_url = mock_url_resolver.file_url
        big_query_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver)
        mock_schema = 'my_project.my_dataset'
        mock_table = Mock(name='mock_table')
        mock_load_plan = Mock(name='mock_load_plan')
        mock_load_plan.records_format = Mock(name='records_format', spec=DelimitedRecordsFormat)
        mock_target_records_format = mock_load_plan.records_format
        mock_target_records_format.format_type = 'delimited'
        mock_target_records_format.hints = {}
        mock_directory = Mock(name='mock_directory')
        mock_url = Mock(name='mock_url')
        mock_directory.manifest_entry_urls.return_value = [mock_url]

        mock_connection = mock_db.engine.raw_connection.return_value.connection
        mock_client = mock_connection._client
        mock_dataset_ref = mock_client.dataset.return_value
        mock_table_ref = mock_dataset_ref.table.return_value
        mock_job = mock_client.load_table_from_file.return_value
        mock_job.output_rows = 42
        mock_loc = mock_file_url.return_value
        mock_f = mock_loc.open.return_value.__enter__.return_value
        out = big_query_loader.load(schema=mock_schema, table=mock_table,
                                    load_plan=mock_load_plan,
                                    directory=mock_directory)
        mock_client.dataset.assert_called_with('my_dataset', 'my_project')
        mock_dataset_ref.table.assert_called_with(mock_table)
        mock_file_url.assert_called_with(mock_url)
        mock_client.load_table_from_file.\
            assert_called_with(mock_f,
                               mock_table_ref,
                               location="US",
                               job_config=mock_load_job_config.return_value)
        mock_job.result.assert_called_with()

        self.assertEqual(out, mock_job.output_rows)

    @patch('records_mover.db.bigquery.loader.load_job_config')
    @patch('records_mover.db.bigquery.loader.ProcessingInstructions')
    @patch('records_mover.db.bigquery.loader.RecordsLoadPlan')
    def test_can_load_this_format_true(self,
                                       mock_RecordsLoadPlan,
                                       mock_ProcessingInstructions,
                                       mock_load_job_config):
        mock_db = Mock(name='db')
        mock_source_records_format = Mock(name='source_records_format', spec=DelimitedRecordsFormat)
        mock_source_records_format.format_type = 'delimited'
        mock_processing_instructions = mock_ProcessingInstructions.return_value
        mock_load_plan = mock_RecordsLoadPlan.return_value
        mock_load_plan.records_format = mock_source_records_format
        mock_url_resolver = Mock(name='url_resolver')
        mock_source_records_format.hints = {}
        bigquery_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver)
        out = bigquery_loader.can_load_this_format(mock_source_records_format)
        mock_ProcessingInstructions.assert_called_with()
        mock_RecordsLoadPlan.\
            assert_called_with(records_format=mock_source_records_format,
                               processing_instructions=mock_processing_instructions)
        mock_load_job_config.assert_called_with(set(), mock_load_plan)
        self.assertEqual(True, out)

    def test_known_supported_records_formats_for_load(self):
        mock_db = Mock(name='db')
        mock_url_resolver = Mock(name='url_resolver')
        bigquery_loader = BigQueryLoader(db=mock_db, url_resolver=mock_url_resolver)
        out = bigquery_loader.known_supported_records_formats_for_load()
        self.assertEqual(2, len(out))
        delimited_records_format = out[0]
        self.assertEqual(type(delimited_records_format), DelimitedRecordsFormat)
        self.assertEqual('bigquery', delimited_records_format.variant)
        parquet_records_format = out[1]
        self.assertEqual(type(parquet_records_format), ParquetRecordsFormat)
