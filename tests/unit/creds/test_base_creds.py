import unittest
import google.auth.credentials
from typing import Iterable
from db_facts.db_facts_types import DBFacts
from unittest.mock import Mock
from records_mover.creds.base_creds import BaseCreds


class ExampleCredsSubclass(BaseCreds):
    def _gcp_creds(self,
                   gcp_creds_name: str,
                   scopes: Iterable[str]) -> 'google.auth.credentials.Credentials':
        mock_gcp_creds = Mock(name='gcp_creds')
        mock_gcp_creds.gcp_creds_name = gcp_creds_name
        mock_gcp_creds.scopes = scopes
        return mock_gcp_creds

    def db_facts(self, db_creds_name: str) -> DBFacts:
        mock_db_facts = Mock(name='db_facts')
        mock_db_facts.db_creds_name = db_creds_name
        return mock_db_facts


class TestBaseCreds(unittest.TestCase):
    maxDiff = None

    def test_gcs(self):
        creds = ExampleCredsSubclass(default_db_creds_name=None,
                                     default_aws_creds_name=None,
                                     default_gcp_creds_name=None)
        mock_gcp_creds_name = Mock(name='gcp_creds_name')
        out = creds.gcs(mock_gcp_creds_name)
        self.assertEqual(out.gcp_creds_name, mock_gcp_creds_name)
        self.assertTupleEqual(out.scopes, (
            'https://www.googleapis.com/auth/devstorage.full_control',
            'https://www.googleapis.com/auth/devstorage.read_only',
            'https://www.googleapis.com/auth/devstorage.read_write'
        ))

    def test_default_default_db_facts_specified(self):
        creds = ExampleCredsSubclass(default_db_creds_name='my_db_creds',
                                     default_aws_creds_name=None,
                                     default_gcp_creds_name=None)
        out = creds.default_db_facts()
        self.assertEqual(out.db_creds_name, 'my_db_creds')
