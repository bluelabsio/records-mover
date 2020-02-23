from mock import patch
import unittest


@patch('records_mover.base_job_context.pathlib')
class TestJobContextRecords(unittest.TestCase):
    def mock_job_context(self, **kwargs):
        from records_mover import job_context

        return job_context.get_job_context(**kwargs)
