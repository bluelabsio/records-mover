import unittest
from mock import Mock, patch
from records_mover.records.job.mover import run_records_mover_job
from records_mover.records.records_format import ParquetRecordsFormat
from contextlib import contextmanager


class TestJobsHintsAndParquetRecordsFormat(unittest.TestCase):
    maxDiff = None

    @patch('records_mover.records.job.mover.Session')
    def test_parquet(self, mock_Session):
        mock_source = Mock(name='source')
        mock_target = Mock(name='target')

        @contextmanager
        def mysource(records_format=None):
            self.assertIsInstance(records_format, ParquetRecordsFormat)
            yield mock_source

        @contextmanager
        def mytarget(db_engine):
            yield mock_target

        mock_job_name = Mock(name='job_name')
        mock_session = mock_Session.return_value
        mock_records = mock_session.records
        mock_records.sources.mysource = mysource
        mock_records.targets.mytarget = mytarget
        config = {
            'fail_if_dont_understand': False,
            'source': {
                'format': 'parquet'
            },
            'target': {
                'db_name': 'foo'
            },
        }
        out = run_records_mover_job(source_method_name='mysource',
                                    target_method_name='mytarget',
                                    job_name=mock_job_name,
                                    config=config)
        mock_records.move.assert_called()
        self.assertEqual(out, mock_records.move.return_value)

    @patch('records_mover.records.job.mover.Session')
    def test_parquet_with_delimited_hint_format_first(self, mock_Session):
        mock_source = Mock(name='source')
        mock_target = Mock(name='target')

        @contextmanager
        def mysource(records_format=None):
            self.assertIsInstance(records_format, ParquetRecordsFormat)
            yield mock_source

        @contextmanager
        def mytarget(db_engine):
            yield mock_target

        mock_job_name = Mock(name='job_name')
        mock_session = mock_Session.return_value
        mock_records = mock_session.records
        mock_records.sources.mysource = mysource
        mock_records.targets.mytarget = mytarget
        config = {
            'fail_if_dont_understand': False,
            'source': {
                'format': 'parquet',
                'compression': 'GZIP',
            },
            'target': {
                'db_name': 'foo'
            },
        }
        with self.assertRaises(NotImplementedError):
            run_records_mover_job(source_method_name='mysource',
                                  target_method_name='mytarget',
                                  job_name=mock_job_name,
                                  config=config)

    @patch('records_mover.records.job.mover.Session')
    def test_parquet_with_delimited_hint_hint_first(self, mock_Session):
        mock_source = Mock(name='source')
        mock_target = Mock(name='target')

        @contextmanager
        def mysource(records_format=None):
            self.assertIsInstance(records_format, ParquetRecordsFormat)
            yield mock_source

        @contextmanager
        def mytarget(db_engine):
            yield mock_target

        mock_job_name = Mock(name='job_name')
        mock_session = mock_Session.return_value
        mock_records = mock_session.records
        mock_records.sources.mysource = mysource
        mock_records.targets.mytarget = mytarget
        config = {
            'fail_if_dont_understand': False,
            'source': {
                'compression': 'GZIP',
                'format': 'parquet',
            },
            'target': {
                'db_name': 'foo'
            },
        }
        with self.assertRaises(NotImplementedError):
            run_records_mover_job(source_method_name='mysource',
                                  target_method_name='mytarget',
                                  job_name=mock_job_name,
                                  config=config)
