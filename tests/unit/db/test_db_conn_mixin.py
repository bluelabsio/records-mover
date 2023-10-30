import pytest
from unittest.mock import MagicMock
from sqlalchemy.engine import Connection, Engine
from records_mover.db.db_conn_mixin import DBConnMixin


class TestDBConnMixin:
    @pytest.fixture
    def mixin(self):
        class TestMixin(DBConnMixin):
            def __init__(self):
                self._db_conn = None
                self.db_engine = MagicMock(spec=Engine)
                self.conn_opened_here = False
        return TestMixin()

    def test_get_db_conn(self, mixin):
        conn = MagicMock(spec=Connection)
        mixin.db_engine.connect.return_value = conn
        assert mixin.get_db_conn() == conn
        assert mixin.conn_opened_here is True

    def test_get_db_conn_with_existing_conn(self, mixin):
        conn = MagicMock(spec=Connection)
        mixin._db_conn = conn
        assert mixin.get_db_conn() == conn
        assert mixin.conn_opened_here is False

    def test_set_db_conn(self, mixin):
        conn = MagicMock(spec=Connection)
        mixin.set_db_conn(conn)
        assert mixin._db_conn == conn

    def test_del_db_conn(self, mixin):
        mixin.conn_opened_here = True
        conn = MagicMock(spec=Connection)
        mixin._db_conn = conn
        mixin.del_db_conn()
        assert mixin._db_conn is None
        assert mixin.conn_opened_here is False

    def test_del_db_conn_with_no_conn(self, mixin):
        mixin.conn_opened_here = True
        mixin.del_db_conn()
        assert mixin._db_conn is None
        assert mixin.conn_opened_here is False
