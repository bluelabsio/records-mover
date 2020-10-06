import unittest
from typing import Optional
from mock import Mock, patch, ANY
from records_mover.records.job.mover import run_records_mover_job
from records_mover.records.records_format import (
    BaseRecordsFormat, DelimitedRecordsFormat
)
from records_mover.records.job.schema import method_to_json_schema


class TestJobs(unittest.TestCase):
    maxDiff = None

    @patch('records_mover.records.job.mover.Session')
    def test_run_records_mover_job(self,
                                   mock_Session):
        mock_source = Mock(name='source')
        mock_target = Mock(name='target')

        def mysource(a: int,
                     schema_name: str,
                     google_cloud_creds,
                     existing_table_handling=None,
                     spectrum_base_url=None):
            self.assertEqual(a, 1)
            self.assertEqual(google_cloud_creds, mock_session.creds.google_sheets.return_value)
            self.assertEqual(schema_name, 'myschema')
            self.assertEqual(spectrum_base_url, 'spectrumschema')
            return mock_source

        def mytarget(b: int, db_engine):
            assert b == 2
            self.assertEqual(db_engine, mock_session.get_db_engine.return_value)
            return mock_target

        mock_job_name = Mock(name='job_name')
        mock_session = mock_Session.return_value
        mock_db_facts = {
            'redshift_spectrum_base_url_myschema': 'spectrumschema'
        }
        mock_session.creds.db_facts.return_value = mock_db_facts
        mock_records = mock_session.records
        mock_records.sources.mysource = mysource
        mock_records.targets.mytarget = mytarget
        config = {
            'fail_if_dont_understand': False,
            'source': {
                'a': 1,
                'db_name': 'mydbname',
                'gcp_creds_name': 'mygcpcreds',
                'existing_table': 'drop_and_recreate',
                'schema_name': 'myschema'
            },
            'target': {
                'b': 2,
                'db_name': 'foo',
            }
        }
        out = run_records_mover_job(source_method_name='mysource',
                                    target_method_name='mytarget',
                                    job_name=mock_job_name,
                                    config=config)
        mock_records.move.assert_called()
        self.assertEqual(out, mock_records.move.return_value)

    @patch('records_mover.records.job.mover.Session')
    def test_run_records_mover_hint_and_then_variant(self,
                                                     mock_Session):
        mock_source = Mock(name='source')
        mock_target = Mock(name='target')

        def mysource():
            return mock_source

        def mytarget(records_format: Optional[BaseRecordsFormat] = None):
            assert isinstance(records_format, DelimitedRecordsFormat)
            self.assertEqual(records_format.variant, 'csv')
            self.assertIsNone(records_format.hints['escape'])
            return mock_target

        mock_job_name = Mock(name='job_name')
        mock_session = mock_Session.return_value
        mock_db_facts = {
            'redshift_spectrum_base_url_myschema': 'spectrumschema'
        }
        mock_session.creds.db_facts.return_value = mock_db_facts
        mock_records = mock_session.records
        mock_records.sources.mysource = mysource
        mock_records.targets.mytarget = mytarget
        config = {
            'fail_if_dont_understand': False,
            'source': {},
            'target': {
                'compression': None,
                'variant': 'csv'
            }
        }
        out = run_records_mover_job(source_method_name='mysource',
                                    target_method_name='mytarget',
                                    job_name=mock_job_name,
                                    config=config)
        mock_records.move.assert_called()
        self.assertEqual(out, mock_records.move.return_value)

    @patch('records_mover.records.job.schema.method_signature_to_json_schema')
    def test_method_to_json_schema(self, mock_method_signature_to_json_schema):
        def foo(a, b, c):
            pass

        out = method_to_json_schema(foo)
        mock_method_signature_to_json_schema.assert_called_with(foo,
                                                                parameters_to_ignore=ANY,
                                                                special_handling=ANY)
        self.assertEqual(mock_method_signature_to_json_schema.return_value,
                         out)
