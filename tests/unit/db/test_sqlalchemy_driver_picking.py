import unittest
from unittest.mock import patch
from records_mover.db import connect


class TestSQLAlchemyDriverPicking(unittest.TestCase):
    @patch('records_mover.db.connect.db_facts_from_lpass')
    @patch('records_mover.db.connect.sa.create_engine')
    def test_create_sqlalchemy_url(self,
                                   mock_create_engine,
                                   mock_db_facts_from_lpass):
        expected_mappings = {
            'psql (redshift)':
            'redshift://myuser:hunter1@myhost:123/analyticsdb?keepalives=1&keepalives_idle=30',

            'redshift':
            'redshift://myuser:hunter1@myhost:123/analyticsdb?keepalives=1&keepalives_idle=30',

            'vertica':
            'vertica+vertica_python://myuser:hunter1@myhost:123/analyticsdb',

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
            self.assertEqual(actual_url_str,expected_url,"{}!={}".format(actual_url_str,
                                                                         expected_url))

    @patch('records_mover.db.connect.db_facts_from_lpass')
    @patch('records_mover.db.connect.sa.create_engine')
    def test_create_sqlalchemy_url_odbc_preferred(self,
                                                  mock_create_engine,
                                                  mock_db_facts_from_lpass):
        expected_mappings = {
            'psql (redshift)':
            'redshift://myuser:hunter1@myhost:123/analyticsdb?keepalives=1&keepalives_idle=30',

            'redshift':
            'redshift://myuser:hunter1@myhost:123/analyticsdb?keepalives=1&keepalives_idle=30',

            'vertica':
            'vertica+pyodbc:///?odbc_connect=Driver'
            '%3DHPVertica%3BSERVER%3Dmyhost%3BDATABASE%3Danalyticsdb'
            '%3BPORT%3D123%3BUID%3Dmyuser%3BPWD%3Dhunter1%3BCHARSET%3DUTF8%3B',

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
            if human_style_db_type == 'redshift':
                db_facts['query'] = {'keepalives': '1', 'keepalives_idle': '30'}
            actual_url = connect.create_sqlalchemy_url(db_facts, prefer_odbc=True)
            assert str(actual_url) == expected_url
