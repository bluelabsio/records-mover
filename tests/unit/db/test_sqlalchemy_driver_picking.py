import unittest
from unittest.mock import patch
from records_mover.db import connect


class TestSQLAlchemyDriverPicking(unittest.TestCase):
    @patch('records_mover.db.connect.sa.create_engine')
    def test_create_sqlalchemy_url(self,
                                   mock_create_engine):
        expected_mappings = {
            'psql (redshift)':
            'redshift://myuser:hunter1@myhost:123/analyticsdb?keepalives=1&keepalives_idle=30',

            'redshift':
            'redshift://myuser:hunter1@myhost:123/analyticsdb?keepalives=1&keepalives_idle=30',

            'psql':
            'postgresql://myuser:hunter1@myhost:123/analyticsdb',

            'postgres':
            'postgresql://myuser:hunter1@myhost:123/analyticsdb',
        }
        for human_style_db_type, expected_url in expected_mappings.items():
            print("Called with " + human_style_db_type)
            db_facts = {
                'password': 'hunter1',
                'host': 'myhost',
                'user': 'myuser',
                'type': human_style_db_type,
                'port': 123,
                'database': 'analyticsdb'
            }
            if human_style_db_type in ['redshift', 'psql (redshift)']:
                db_facts['query'] = {'keepalives': '1', 'keepalives_idle': '30'}
            actual_url = connect.create_sqlalchemy_url(db_facts)
            actual_url_str = str(actual_url)
            self.assertEqual(actual_url_str, expected_url, "{}!={}".format(actual_url_str,
                                                                           expected_url))
