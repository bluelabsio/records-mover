import unittest
from http import HTTPStatus
from unittest.mock import patch, MagicMock

from records_mover.creds.base_creds import BaseCreds
from records_mover.records.airbyte.airbyte import AirbyteEngine


class MockCreds(BaseCreds):
    """
    Helper class to provide credentials to the airbyte engine
    """

    def airbyte(self):
        return {
            'user': 'username',
            'password': 'password',
            'host': 'host',
            'port': '8000',
            'endpoint': 'endpoint'
        }


class MockSession:
    """
    Helper class to provide credentials, just so we don't have to worry about db-facts
    """

    def __init__(self):
        self.creds = MockCreds()

    def set_stream_logging(self):
        pass


class AirbyteHealthCheckTest(unittest.TestCase):
    @patch('requests.Session.send')
    def test_airbyte_healthcheck_returns_true_when_healthy(self, send):
        # Given
        engine = AirbyteEngine(session=MockSession())
        response = MagicMock()
        response.status_code = HTTPStatus.OK.value
        send.return_value = response

        # When
        result = engine.healthcheck()

        # Then
        self.assertTrue(result)

    @patch('requests.Session.send')
    def test_airbyte_healthcheck_returns_false_when_unhealthy(self, send):
        # Given
        engine = AirbyteEngine(session=MockSession())
        response = MagicMock()
        response.status_code = HTTPStatus.NOT_FOUND.value
        send.return_value = response

        # When
        result = engine.healthcheck()

        # Then
        self.assertFalse(result)
