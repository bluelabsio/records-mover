from contextlib import contextmanager
from mock import patch
import unittest


@patch('records_mover.base_job_context.pathlib')
class TestJobContextRecords(unittest.TestCase):
    @contextmanager
    def mock_job_context(self, **kwargs):
        from records_mover import job_context

        with job_context.create_job_context('dummy_job', **kwargs) as context:
            yield context
