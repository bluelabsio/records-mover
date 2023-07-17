import unittest
from mock import Mock, MagicMock, patch, call
from records_mover.records.prep import TablePrep
from records_mover.records.existing_table_handling import ExistingTableHandling
from records_mover.db import DBDriver


class TestPrep(unittest.TestCase):
    def setUp(self):
        self.mock_tbl = Mock(name='target_table_details')
        self.prep = TablePrep(self.mock_tbl)

    @patch('records_mover.records.prep.quote_schema_and_table')
    def test_prep_table_exists_append_implicit(self, mock_quote_schema_and_table):
        mock_schema_sql = Mock(name='schema_sql')
        mock_driver = Mock(name='driver', spec=DBDriver)
        mock_driver.db_conn = MagicMock(name='db')
        mock_driver.db_engine = Mock(name='db_endine')

        mock_driver.has_table.return_value = True
        how_to_prep = ExistingTableHandling.APPEND
        self.mock_tbl.existing_table_handling = how_to_prep

        self.prep.prep(mock_schema_sql, mock_driver)

    @patch('records_mover.records.prep.quote_schema_and_table')
    def test_prep_table_exists_truncate_implicit(self, mock_quote_schema_and_table):
        mock_schema_sql = Mock(name='schema_sql')
        mock_driver = Mock(name='driver', spec=DBDriver)
        mock_driver.db_conn = MagicMock(name='db')
        mock_driver.db_engine = Mock(name='db_endine')

        mock_quote_schema_and_table
        mock_driver.has_table.return_value = True
        how_to_prep = ExistingTableHandling.TRUNCATE_AND_OVERWRITE
        mock_schema_and_table = mock_quote_schema_and_table.return_value
        self.mock_tbl.existing_table_handling = how_to_prep

        self.prep.prep(mock_schema_sql, mock_driver)

        mock_quote_schema_and_table.assert_called_with(mock_driver.db,
                                                       self.mock_tbl.schema_name,
                                                       self.mock_tbl.table_name)
        mock_driver.db.execute.assert_called_with(f"TRUNCATE TABLE {mock_schema_and_table}")

    @patch('records_mover.records.prep.quote_schema_and_table')
    def test_prep_table_exists_delete_implicit(self, mock_quote_schema_and_table):
        mock_schema_sql = Mock(name='schema_sql')
        mock_driver = Mock(name='driver', spec=DBDriver)
        mock_driver.db_conn = MagicMock(name='db')
        mock_driver.db_engine = Mock(name='db_endine')

        mock_quote_schema_and_table
        mock_driver.has_table.return_value = True
        how_to_prep = ExistingTableHandling.DELETE_AND_OVERWRITE
        mock_schema_and_table = mock_quote_schema_and_table.return_value
        self.mock_tbl.existing_table_handling = how_to_prep

        self.prep.prep(mock_schema_sql, mock_driver)

        mock_quote_schema_and_table.assert_called_with(mock_driver.db,
                                                       self.mock_tbl.schema_name,
                                                       self.mock_tbl.table_name)
        mock_driver.db.execute.assert_called_with(f"DELETE FROM {mock_schema_and_table} WHERE true")

    @patch('records_mover.records.prep.quote_schema_and_table')
    def test_prep_table_exists_drop_implicit(self, mock_quote_schema_and_table):
        mock_schema_sql = Mock(name='schema_sql')
        mock_driver = Mock(name='driver', spec=DBDriver)
        mock_db = MagicMock(name='db')
        mock_driver.db_conn = mock_db

        mock_quote_schema_and_table
        mock_driver.has_table.return_value = True
        how_to_prep = ExistingTableHandling.DROP_AND_RECREATE
        mock_schema_and_table = mock_quote_schema_and_table.return_value
        self.mock_tbl.existing_table_handling = how_to_prep

        self.prep.prep(mock_schema_sql, mock_driver)
        mock_conn = mock_db.engine.connect.return_value.__enter__.return_value

        mock_quote_schema_and_table.assert_called_with(mock_driver.db,
                                                       self.mock_tbl.schema_name,
                                                       self.mock_tbl.table_name)
        mock_conn.execute.assert_has_calls([
            call(f"DROP TABLE {mock_schema_and_table}"),
            call(mock_schema_sql),
        ])
        mock_driver.set_grant_permissions_for_groups.\
            assert_called_with(self.mock_tbl.schema_name,
                               self.mock_tbl.table_name,
                               self.mock_tbl.add_group_perms_for,
                               mock_conn)
        mock_driver.set_grant_permissions_for_users.\
            assert_called_with(self.mock_tbl.schema_name,
                               self.mock_tbl.table_name,
                               self.mock_tbl.add_user_perms_for,
                               mock_conn)

    @patch('records_mover.records.prep.quote_schema_and_table')
    def test_prep_table_not_exists(self, mock_quote_schema_and_table):
        mock_schema_sql = Mock(name='schema_sql')
        mock_driver = Mock(name='driver', spec=DBDriver)
        mock_db = MagicMock(name='db')
        mock_driver.db_conn = mock_db

        mock_quote_schema_and_table
        mock_driver.has_table.return_value = False
        how_to_prep = ExistingTableHandling.DROP_AND_RECREATE
        self.mock_tbl.existing_table_handling = how_to_prep

        self.prep.prep(mock_schema_sql, mock_driver)
        mock_conn = mock_db.engine.connect.return_value.__enter__.return_value

        mock_conn.execute.assert_has_calls([
            call(mock_schema_sql),
        ])
        mock_driver.set_grant_permissions_for_groups.\
            assert_called_with(self.mock_tbl.schema_name,
                               self.mock_tbl.table_name,
                               self.mock_tbl.add_group_perms_for,
                               mock_conn)
        mock_driver.set_grant_permissions_for_users.\
            assert_called_with(self.mock_tbl.schema_name,
                               self.mock_tbl.table_name,
                               self.mock_tbl.add_user_perms_for,
                               mock_conn)

    @patch('records_mover.records.prep.quote_schema_and_table')
    def test_prep_table_exists_drop_explicit(self, mock_quote_schema_and_table):
        mock_schema_sql = Mock(name='schema_sql')
        mock_driver = Mock(name='driver', spec=DBDriver)
        mock_db = MagicMock(name='db')
        mock_driver.db_conn = mock_db

        mock_quote_schema_and_table
        mock_driver.has_table.return_value = True
        how_to_prep = ExistingTableHandling.DELETE_AND_OVERWRITE
        mock_schema_and_table = mock_quote_schema_and_table.return_value
        self.mock_tbl.existing_table_handling = how_to_prep

        self.prep.prep(mock_schema_sql, mock_driver,
                       existing_table_handling=ExistingTableHandling.DROP_AND_RECREATE)
        mock_conn = mock_db.engine.connect.return_value.__enter__.return_value

        mock_quote_schema_and_table.assert_called_with(mock_driver.db,
                                                       self.mock_tbl.schema_name,
                                                       self.mock_tbl.table_name)
        mock_conn.execute.assert_has_calls([
            call(f"DROP TABLE {mock_schema_and_table}"),
            call(mock_schema_sql),
        ])
        mock_driver.set_grant_permissions_for_groups.\
            assert_called_with(self.mock_tbl.schema_name,
                               self.mock_tbl.table_name,
                               self.mock_tbl.add_group_perms_for,
                               mock_conn)
        mock_driver.set_grant_permissions_for_users.\
            assert_called_with(self.mock_tbl.schema_name,
                               self.mock_tbl.table_name,
                               self.mock_tbl.add_user_perms_for,
                               mock_conn)
