import unittest

from records_mover.db.bigquery.loader import BigQueryLoader
from records_mover.records.records_format import (
    DelimitedRecordsFormat, ParquetRecordsFormat, AvroRecordsFormat,
    BaseRecordsFormat
)
from mock import Mock
from unittest.mock import patch


class NewRecordsFormat(BaseRecordsFormat):
    ...


class TestBigQueryLoaderCanLoadThisFormat(unittest.TestCase):
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
        bigquery_loader = BigQueryLoader(db=None, url_resolver=mock_url_resolver,
                                         gcs_temp_base_loc=None, db_conn=mock_db)
        out = bigquery_loader.can_load_this_format(mock_source_records_format)
        mock_ProcessingInstructions.assert_called_with()
        mock_RecordsLoadPlan.\
            assert_called_with(records_format=mock_source_records_format,
                               processing_instructions=mock_processing_instructions)
        mock_load_job_config.assert_called_with(set(), mock_load_plan)
        self.assertEqual(True, out)

    @patch('records_mover.db.bigquery.loader.load_job_config')
    @patch('records_mover.db.bigquery.loader.ProcessingInstructions')
    @patch('records_mover.db.bigquery.loader.RecordsLoadPlan')
    def test_can_load_this_format_delimited_false(self,
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
        mock_load_job_config.side_effect = NotImplementedError
        mock_source_records_format.hints = {}
        bigquery_loader = BigQueryLoader(db=None, url_resolver=mock_url_resolver,
                                         gcs_temp_base_loc=None, db_conn=mock_db)
        out = bigquery_loader.can_load_this_format(mock_source_records_format)
        mock_ProcessingInstructions.assert_called_with()
        mock_RecordsLoadPlan.\
            assert_called_with(records_format=mock_source_records_format,
                               processing_instructions=mock_processing_instructions)
        mock_load_job_config.assert_called_with(set(), mock_load_plan)
        self.assertEqual(False, out)

    @patch('records_mover.db.bigquery.loader.load_job_config')
    @patch('records_mover.db.bigquery.loader.ProcessingInstructions')
    @patch('records_mover.db.bigquery.loader.RecordsLoadPlan')
    def test_can_load_this_format_true_avro(self,
                                            mock_RecordsLoadPlan,
                                            mock_ProcessingInstructions,
                                            mock_load_job_config):
        mock_db = Mock(name='db')
        mock_source_records_format = Mock(name='source_records_format', spec=AvroRecordsFormat)
        mock_source_records_format.format_type = 'avro'
        mock_processing_instructions = mock_ProcessingInstructions.return_value
        mock_load_plan = mock_RecordsLoadPlan.return_value
        mock_load_plan.records_format = mock_source_records_format
        mock_url_resolver = Mock(name='url_resolver')
        mock_source_records_format.hints = {}
        bigquery_loader = BigQueryLoader(db=None, url_resolver=mock_url_resolver,
                                         gcs_temp_base_loc=None, db_conn=mock_db)
        out = bigquery_loader.can_load_this_format(mock_source_records_format)
        mock_ProcessingInstructions.assert_called_with()
        mock_RecordsLoadPlan.\
            assert_called_with(records_format=mock_source_records_format,
                               processing_instructions=mock_processing_instructions)
        self.assertTrue(out)

    @patch('records_mover.db.bigquery.loader.load_job_config')
    @patch('records_mover.db.bigquery.loader.ProcessingInstructions')
    @patch('records_mover.db.bigquery.loader.RecordsLoadPlan')
    def test_can_load_this_format_false_newformat(self,
                                                  mock_RecordsLoadPlan,
                                                  mock_ProcessingInstructions,
                                                  mock_load_job_config):
        mock_db = Mock(name='db')
        mock_source_records_format = Mock(name='source_records_format', spec=NewRecordsFormat)
        mock_source_records_format.format_type = 'new'
        mock_processing_instructions = mock_ProcessingInstructions.return_value
        mock_load_plan = mock_RecordsLoadPlan.return_value
        mock_load_plan.records_format = mock_source_records_format
        mock_url_resolver = Mock(name='url_resolver')
        mock_source_records_format.hints = {}
        bigquery_loader = BigQueryLoader(db=None, url_resolver=mock_url_resolver,
                                         gcs_temp_base_loc=None, db_conn=mock_db)
        out = bigquery_loader.can_load_this_format(mock_source_records_format)
        mock_ProcessingInstructions.assert_called_with()
        mock_RecordsLoadPlan.\
            assert_called_with(records_format=mock_source_records_format,
                               processing_instructions=mock_processing_instructions)
        self.assertFalse(out)

    @patch('records_mover.db.bigquery.loader.load_job_config')
    @patch('records_mover.db.bigquery.loader.ProcessingInstructions')
    @patch('records_mover.db.bigquery.loader.RecordsLoadPlan')
    def test_can_load_this_format_true_parquet(self,
                                               mock_RecordsLoadPlan,
                                               mock_ProcessingInstructions,
                                               mock_load_job_config):
        mock_db = Mock(name='db')
        mock_source_records_format = Mock(name='source_records_format', spec=ParquetRecordsFormat)
        mock_source_records_format.format_type = 'parquet'
        mock_processing_instructions = mock_ProcessingInstructions.return_value
        mock_load_plan = mock_RecordsLoadPlan.return_value
        mock_load_plan.records_format = mock_source_records_format
        mock_url_resolver = Mock(name='url_resolver')
        mock_source_records_format.hints = {}
        bigquery_loader = BigQueryLoader(db=None, url_resolver=mock_url_resolver,
                                         gcs_temp_base_loc=None, db_conn=mock_db)
        out = bigquery_loader.can_load_this_format(mock_source_records_format)
        mock_ProcessingInstructions.assert_called_with()
        mock_RecordsLoadPlan.\
            assert_called_with(records_format=mock_source_records_format,
                               processing_instructions=mock_processing_instructions)
        self.assertTrue(out)
