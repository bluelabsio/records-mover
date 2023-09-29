import unittest
from unittest.mock import MagicMock
from records_mover import check_db_conn_engine
import sqlalchemy


class TestCheckDbConnEngine(unittest.TestCase):
    def test_provide_db_only_db_is_engine(self):
        mock_db = MagicMock(name='db', spec=sqlalchemy.engine.Engine)
        with self.assertWarns(DeprecationWarning):
            db, db_conn, db_engine = check_db_conn_engine(db=mock_db)
        self.assertEqual(mock_db, db)
        self.assertEqual(mock_db, db_engine)
        assert not db_conn

    def test_provide_db_only_db_is_conn(self):
        mock_db = MagicMock(name='db', spec=sqlalchemy.engine.Connection)
        mock_db_engine = MagicMock(name='db_engine', spec=sqlalchemy.engine.Engine)
        mock_db.engine = mock_db_engine
        with self.assertWarns(DeprecationWarning):
            db, db_conn, db_engine = check_db_conn_engine(db=mock_db)
        self.assertEqual(mock_db, db)
        self.assertEqual(mock_db, db_conn)
        self.assertEqual(mock_db_engine, db_engine)

    def test_provide_db_conn_only(self):
        mock_db_conn = MagicMock(name='db_conn', spec=sqlalchemy.engine.Connection)
        mock_db_engine = MagicMock(name='db_engine', spec=sqlalchemy.engine.Engine)
        mock_db_conn.engine = mock_db_engine
        db, db_conn, db_engine = check_db_conn_engine(db_conn=mock_db_conn)
        self.assertEqual(mock_db_conn, db_conn)
        self.assertEqual(mock_db_engine, db_engine)
        assert not db

    def test_provide_db_engine_only(self):
        mock_db_engine = MagicMock(name='db_engine', spec=sqlalchemy.engine.Engine)
        db, db_conn, db_engine = check_db_conn_engine(db_engine=mock_db_engine)
        self.assertEqual(mock_db_engine, db_engine)
        assert not db
        assert not db_conn

    def test_provide_nothing(self):
        with self.assertRaises(ValueError):
            check_db_conn_engine(db=None, db_conn=None, db_engine=None)

    def test_provide_db_and_db_conn(self):
        mock_db = MagicMock(name='db', spec=sqlalchemy.engine.Engine)
        mock_db_conn = MagicMock(name='db_conn', spec=sqlalchemy.engine.Connection)
        with self.assertWarns(DeprecationWarning):
            db, db_conn, db_engine = check_db_conn_engine(db=mock_db, db_conn=mock_db_conn)
        self.assertEqual(mock_db, db)
        self.assertEqual(mock_db_conn, db_conn)
        self.assertEqual(mock_db, db_engine)

    def test_provide_db_and_db_engine(self):
        mock_db = MagicMock(name='db', spec=sqlalchemy.engine.Engine)
        mock_db_engine = MagicMock(name='db_engine', spec=sqlalchemy.engine.Engine)
        with self.assertWarns(DeprecationWarning):
            db, db_conn, db_engine = check_db_conn_engine(db=mock_db, db_engine=mock_db_engine)
        self.assertEqual(mock_db, db)
        self.assertEqual(mock_db_engine, db_engine)
        assert not db_conn

    def test_provide_db_conn_and_db_engine(self):
        mock_db_conn = MagicMock(name='db_conn', spec=sqlalchemy.engine.Connection)
        mock_db_engine = MagicMock(name='db_engine', spec=sqlalchemy.engine.Engine)
        mock_db_conn.engine = mock_db_engine
        db, db_conn, db_engine = check_db_conn_engine(db_conn=mock_db_conn,
                                                      db_engine=mock_db_engine)
        self.assertEqual(mock_db_conn, db_conn)
        self.assertEqual(mock_db_engine, db_engine)
        assert not db

    def test_provide_db_and_db_conn_and_db_engine(self):
        mock_db = MagicMock(name='db', spec=sqlalchemy.engine.Engine)
        mock_db_conn = MagicMock(name='db_conn', spec=sqlalchemy.engine.Connection)
        mock_db_engine = MagicMock(name='db_engine', spec=sqlalchemy.engine.Engine)
        with self.assertWarns(DeprecationWarning):
            db, db_conn, db_engine = check_db_conn_engine(db=mock_db,
                                                          db_conn=mock_db_conn,
                                                          db_engine=mock_db_engine)
        self.assertEqual(mock_db, db)
        self.assertEqual(mock_db_conn, db_conn)
        self.assertEqual(mock_db_engine, db_engine)
