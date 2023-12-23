import unittest
from unittest.mock import patch
from records_mover.db import connect


class TestConnect(unittest.TestCase):
    @patch('records_mover.db.connect.db_info_from_lpass')
    @patch('records_mover.db.connect.sa.create_engine')
    @patch('records_mover.db.connect.sa.engine.url.URL.create')
    def test_engine_from_lpass_entry(self,
                                     mock_url,
                                     mock_create_engine,
                                     mock_db_info_from_lpass):
        mock_db_info_from_lpass.return_value = {
            'password': 'hunter1',
            'host': 'myhost',
            'user': 'myuser',
            'type': 'psql (redshift)',
            'port': 123,
            'database': 'analyticsdb'
        }
        engine = connect.engine_from_lpass_entry('my_lpass_name')
        mock_db_info_from_lpass.assert_called_with('my_lpass_name')
        mock_url.assert_called_with(database='analyticsdb',
                                    drivername='redshift',
                                    host='myhost',
                                    password='hunter1',
                                    port=123,
                                    username='myuser',
                                    query={'keepalives': '1',
                                           'keepalives_idle': '30'})
        mock_create_engine.\
            assert_called_with(mock_url.return_value)
        assert engine == mock_create_engine.return_value

    @patch('records_mover.db.connect.db_info_from_lpass')
    @patch('records_mover.db.connect.sa.create_engine')
    @patch('records_mover.db.connect.sa.engine.url.URL')
    def test_creating_bigquery_url(self,
                                   mock_url,
                                   mock_create_engine,
                                   mock_db_info_from_lpass):
        db_facts = {
            'type': 'bigquery',
            'bq_default_project_id': 'bluelabs-tools-dev',
        }
        url = connect.create_sqlalchemy_url(db_facts)
        self.assertEqual(url, 'bigquery://bluelabs-tools-dev')

    @patch('records_mover.db.connect.db_info_from_lpass')
    @patch('records_mover.db.connect.sa.create_engine')
    @patch('records_mover.db.connect.sa.engine.url.URL')
    def test_creating_bigquery_url_with_dataset(self,
                                                mock_url,
                                                mock_create_engine,
                                                mock_db_info_from_lpass):
        db_facts = {
            'type': 'bigquery',
            'bq_default_project_id': 'bluelabs-tools-dev',
            'bq_default_dataset_id': 'myfancydataset',
        }
        url = connect.create_sqlalchemy_url(db_facts)
        self.assertEqual(url, 'bigquery://bluelabs-tools-dev/myfancydataset')

    @patch('records_mover.db.connect.db_info_from_lpass')
    @patch('records_mover.db.connect.sa.engine.create_engine')
    @patch('records_mover.db.connect.sa.engine.url.URL')
    def test_creating_bigquery_db_engine(self,
                                         mock_url,
                                         mock_create_engine,
                                         mock_db_info_from_lpass):
        db_facts = {
            'type': 'bigquery',
            'bq_default_project_id': 'bluelabs-tools-dev',
            'bq_default_dataset_id': 'myfancydataset',
        }
        engine = connect.engine_from_db_facts(db_facts)
        self.assertEqual(engine, mock_create_engine.return_value)
