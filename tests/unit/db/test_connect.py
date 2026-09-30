import unittest
from unittest.mock import patch
from records_mover.db import connect


class TestConnect(unittest.TestCase):
    @patch('records_mover.db.connect.sa.create_engine')
    @patch('records_mover.db.connect.sa.engine.url.URL')
    def test_creating_bigquery_url(self,
                                   mock_url,
                                   mock_create_engine):
        db_facts = {
            'type': 'bigquery',
            'bq_default_project_id': 'bluelabs-tools-dev',
        }
        url = connect.create_sqlalchemy_url(db_facts)
        self.assertEqual(url, 'bigquery://bluelabs-tools-dev')

    @patch('records_mover.db.connect.sa.create_engine')
    @patch('records_mover.db.connect.sa.engine.url.URL')
    def test_creating_bigquery_url_with_dataset(self,
                                                mock_url,
                                                mock_create_engine):
        db_facts = {
            'type': 'bigquery',
            'bq_default_project_id': 'bluelabs-tools-dev',
            'bq_default_dataset_id': 'myfancydataset',
        }
        url = connect.create_sqlalchemy_url(db_facts)
        self.assertEqual(url, 'bigquery://bluelabs-tools-dev/myfancydataset')

    @patch('records_mover.db.connect.sa.engine.create_engine')
    @patch('records_mover.db.connect.sa.engine.url.URL')
    def test_creating_bigquery_db_engine(self,
                                         mock_url,
                                         mock_create_engine):
        db_facts = {
            'type': 'bigquery',
            'bq_default_project_id': 'bluelabs-tools-dev',
            'bq_default_dataset_id': 'myfancydataset',
        }
        engine = connect.engine_from_db_facts(db_facts)
        self.assertEqual(engine, mock_create_engine.return_value)
