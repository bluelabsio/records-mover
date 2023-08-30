import unittest
from unittest.mock import patch, Mock
from records_mover.db.factory import db_driver


class TestFactory(unittest.TestCase):
    @patch('records_mover.db.vertica.vertica_db_driver.VerticaDBDriver')
    def test_db_driver_vertica(self,
                               mock_VerticaDBDriver):
        mock_db = Mock(name='db')
        mock_engine = mock_db.engine
        mock_engine.name = 'vertica'
        out = db_driver(None, db_conn=mock_db)
        self.assertEqual(out, mock_VerticaDBDriver.return_value)

    @patch('records_mover.db.redshift.redshift_db_driver.RedshiftDBDriver')
    def test_db_driver_redshift(self,
                                mock_RedshiftDBDriver):
        mock_db = Mock(name='db')
        mock_engine = mock_db.engine
        mock_engine.name = 'redshift'
        out = db_driver(None, db_conn=mock_db)
        self.assertEqual(out, mock_RedshiftDBDriver.return_value)

    @patch('records_mover.db.bigquery.bigquery_db_driver.BigQueryDBDriver')
    def test_db_driver_bigquery(self,
                                mock_BigQueryDBDriver):
        mock_db = Mock(name='db')
        mock_engine = mock_db.engine
        mock_engine.name = 'bigquery'
        out = db_driver(None, db_conn=mock_db)
        self.assertEqual(out, mock_BigQueryDBDriver.return_value)

    @patch('records_mover.db.factory.GenericDBDriver')
    def test_db_driver_other(self,
                             mock_GenericDBDriver):
        mock_db = Mock(name='db')
        mock_engine = mock_db.engine
        mock_engine.name = 'somaskdfaksjf'
        out = db_driver(None, db_conn=mock_db)
        self.assertEqual(out, mock_GenericDBDriver.return_value)
