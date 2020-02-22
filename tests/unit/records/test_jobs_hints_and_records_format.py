import unittest
from odictliteral import odict
from mock import Mock, patch
from records_mover.records.job.mover import run_records_mover_job
from contextlib import contextmanager


class TestJobsHintsAndRecordsFormat(unittest.TestCase):
    maxDiff = None

    @patch('records_mover.records.job.mover.create_job_context')
    def test_initial_hints_only(self, mock_create_job_context):
        mock_source = Mock(name='source')
        mock_target = Mock(name='target')

        @contextmanager
        def mysource(initial_hints=None):
            self.assertTrue(initial_hints is not None)
            self.assertEqual(',', initial_hints['field-delimiter'])
            self.assertEqual('GZIP', initial_hints['compression'])
            self.assertEqual('all', initial_hints['quoting'])
            yield mock_source

        @contextmanager
        def mytarget(db_engine):
            yield mock_target

        mock_job_name = Mock(name='job_name')
        mock_job_context = mock_create_job_context.return_value.__enter__.return_value
        mock_records = mock_job_context.records
        mock_records.sources.mysource = mysource
        mock_records.targets.mytarget = mytarget
        config = {
            'fail_if_dont_understand': False,
            'source': {
                'field-delimiter': ',',
                'compression': 'GZIP',
                'quoting': 'all',
            },
            'target': {
                'db_name': 'foo',
            }
        }
        out = run_records_mover_job(source_method_name='mysource',
                                    target_method_name='mytarget',
                                    job_name=mock_job_name,
                                    config=config)
        mock_records.move.assert_called()
        self.assertEqual(out, mock_records.move.return_value)

    @patch('records_mover.records.job.mover.create_job_context')
    def test_hints_with_no_variant(self, mock_create_job_context):
        mock_source = Mock(name='source')
        mock_target = Mock(name='target')

        @contextmanager
        def mysource(initial_hints=None,
                     records_format=None):
            self.assertTrue(initial_hints is not None)
            self.assertEqual(',', initial_hints['field-delimiter'])
            self.assertEqual('GZIP', initial_hints['compression'])
            self.assertEqual('all', initial_hints['quoting'])
            self.assertIsNone(records_format)
            yield mock_source

        @contextmanager
        def mytarget(db_engine):
            yield mock_target

        mock_job_name = Mock(name='job_name')
        mock_job_context = mock_create_job_context.return_value.__enter__.return_value
        mock_records = mock_job_context.records
        mock_records.sources.mysource = mysource
        mock_records.targets.mytarget = mytarget
        config = {
            'fail_if_dont_understand': False,
            'source': {
                'field-delimiter': ',',
                'compression': 'GZIP',
                'quoting': 'all',
            },
            'target': {
                'db_name': 'foo'
            }
        }
        out = run_records_mover_job(source_method_name='mysource',
                                    target_method_name='mytarget',
                                    job_name=mock_job_name,
                                    config=config)
        mock_records.move.assert_called()
        self.assertEqual(out, mock_records.move.return_value)

    @patch('records_mover.records.job.mover.create_job_context')
    def test_variant_with_no_hints(self, mock_create_job_context):
        mock_source = Mock(name='source')
        mock_target = Mock(name='target')

        @contextmanager
        def mysource(initial_hints=None,
                     records_format=None):
            self.assertIsNone(initial_hints)
            self.assertEqual(records_format.variant, 'vertica')
            yield mock_source

        @contextmanager
        def mytarget(db_engine):
            yield mock_target

        mock_job_name = Mock(name='job_name')
        mock_job_context = mock_create_job_context.return_value.__enter__.return_value
        mock_records = mock_job_context.records
        mock_records.sources.mysource = mysource
        mock_records.targets.mytarget = mytarget
        config = {
            'fail_if_dont_understand': False,
            'source': {
                'variant': 'vertica',
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

    @patch('records_mover.records.job.mover.create_job_context')
    def test_variant_first_then_hints(self, mock_create_job_context):
        mock_source = Mock(name='source')
        mock_target = Mock(name='target')

        @contextmanager
        def mysource(initial_hints=None,
                     records_format=None):
            self.assertIsNone(initial_hints)
            self.assertEqual(records_format.variant, 'vertica')
            self.assertEqual(',', records_format.hints['field-delimiter'])
            self.assertEqual('GZIP', records_format.hints['compression'])
            self.assertEqual('all', records_format.hints['quoting'])

            yield mock_source

        @contextmanager
        def mytarget(db_engine):
            yield mock_target

        mock_job_name = Mock(name='job_name')
        mock_job_context = mock_create_job_context.return_value.__enter__.return_value
        mock_records = mock_job_context.records
        mock_records.sources.mysource = mysource
        mock_records.targets.mytarget = mytarget
        config = {
            'fail_if_dont_understand': False,
            'source': odict[
                'variant': 'vertica',
                'field-delimiter': ',',
                'compression': 'GZIP',
                'quoting': 'all',
            ],
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

    @patch('records_mover.records.job.mover.create_job_context')
    def test_hints_first_then_variant(self, mock_create_job_context):
        mock_source = Mock(name='source')
        mock_target = Mock(name='target')

        @contextmanager
        def mysource(initial_hints=None,
                     records_format=None):
            self.assertIsNone(initial_hints)
            self.assertEqual(records_format.variant, 'vertica')
            self.assertEqual(',', records_format.hints['field-delimiter'])
            self.assertEqual('GZIP', records_format.hints['compression'])
            self.assertEqual('all', records_format.hints['quoting'])

            yield mock_source

        @contextmanager
        def mytarget(db_engine):
            yield mock_target

        mock_job_name = Mock(name='job_name')
        mock_job_context = mock_create_job_context.return_value.__enter__.return_value
        mock_records = mock_job_context.records
        mock_records.sources.mysource = mysource
        mock_records.targets.mytarget = mytarget
        config = {
            'fail_if_dont_understand': False,
            'source': odict[
                'field-delimiter': ',',
                'compression': 'GZIP',
                'quoting': 'all',
                'variant': 'vertica',
            ],
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

    @patch('records_mover.records.job.mover.create_job_context')
    def test_hints_with_no_initial_hints(self, mock_create_job_context):
        mock_source = Mock(name='source')
        mock_target = Mock(name='target')

        @contextmanager
        def mysource(records_format=None):
            self.assertEqual(records_format.variant, 'bluelabs')
            self.assertEqual(',', records_format.hints['field-delimiter'])
            self.assertEqual('GZIP', records_format.hints['compression'])
            self.assertEqual('all', records_format.hints['quoting'])
            yield mock_source

        @contextmanager
        def mytarget(db_engine):
            yield mock_target

        mock_job_name = Mock(name='job_name')
        mock_job_context = mock_create_job_context.return_value.__enter__.return_value
        mock_records = mock_job_context.records
        mock_records.sources.mysource = mysource
        mock_records.targets.mytarget = mytarget
        config = {
            'fail_if_dont_understand': False,
            'source': {
                'field-delimiter': ',',
                'compression': 'GZIP',
                'quoting': 'all',
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
