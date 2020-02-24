from mock import patch
import unittest


@patch('records_mover.session.pathlib')
class TestSessionRecords(unittest.TestCase):
    def mock_session(self, **kwargs):
        from records_mover import session

        return session.Session(**kwargs)
