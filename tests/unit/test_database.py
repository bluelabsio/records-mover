from records_mover.database import db_engine
from records_mover.db.connect import create_db_url
from records_mover import Session


from mock import patch
import unittest


class TestDatabase(unittest.TestCase):
    @patch('sqlalchemy.create_engine')
    @patch.dict('os.environ', {
        'DB_HOST': 'db.host',
        'DB_USERNAME': 'username',
        'DB_PASSWORD': 'password',
        'DB_TYPE': 'vertica',
        'DB_PORT': '5433',
        'DB_DATABASE': 'analytics',
    })
    def test_database_local_env(self, mock_create_engine):
        context = Session(session_type='cli',
                          default_db_creds_name=None,
                          default_aws_creds_name=None)
        db_facts = {
            "host": "db.host",
            "user": "username",
            "password": "password",
            "port": "5433",
            "database": "analytics",
            "type": "vertica"
            }

        db_url = create_db_url(db_facts)

        db_engine(context)
        mock_create_engine.assert_called_with(db_url)

    @patch.dict('os.environ', {
        'DB_TYPE': 'bigquery',
        'BQ_DEFAULT_PROJECT_ID': 'project_id',
        'BQ_DEFAULT_DATASET_ID': 'dataset_id',
        'BQ_SERVICE_ACCOUNT_JSON': '{"client_email": "blah"}',
    })
    @patch('records_mover.db.connect.sa')
    def test_database_local_env_bigquery(self, mock_sa):
        context = Session(session_type='cli',
                          default_db_creds_name=None,
                          default_aws_creds_name=None)
        db_engine(context)
        mock_sa.engine.create_engine.assert_called_with('bigquery://project_id/dataset_id',
                                                        credentials_info={
                                                            'client_email': 'blah'
                                                        })
