from records_mover.database import db_facts_from_env


from mock import patch
import unittest


class TestDatabase(unittest.TestCase):
    @patch.dict('os.environ', {
        'DB_HOST': 'db.host',
        'DB_USERNAME': 'username',
        'DB_PASSWORD': 'password',
        'DB_TYPE': 'vertica',
        'DB_PORT': '5433',
        'DB_DATABASE': 'analytics',
    })
    def test_db_facts_from_env(self):
        expected_db_facts = {
            "host": "db.host",
            "user": "username",
            "password": "password",
            "port": "5433",
            "database": "analytics",
            "type": "vertica"
            }

        actual_db_facts = db_facts_from_env()
        self.assertEqual(expected_db_facts, actual_db_facts)

    @patch.dict('os.environ', {
        'DB_TYPE': 'bigquery',
        'BQ_DEFAULT_PROJECT_ID': 'project_id',
        'BQ_DEFAULT_DATASET_ID': 'dataset_id',
        'BQ_SERVICE_ACCOUNT_JSON': '{"client_email": "blah"}',
    })
    @patch('records_mover.db.connect.sa')
    def test_database_local_env_bigquery(self, mock_sa):
        expected_db_facts = {
            'bq_default_dataset_id': 'dataset_id',
            'bq_default_project_id': 'project_id',
            'bq_service_account_json': '{"client_email": "blah"}',
            'type': 'bigquery'
        }
        actual_db_facts = db_facts_from_env()
        self.assertEqual(expected_db_facts, actual_db_facts)
