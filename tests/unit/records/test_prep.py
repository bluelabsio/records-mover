import unittest
from mock import Mock, MagicMock, patch
from records_mover.records.prep import TablePrep
from records_mover.records.existing_table_handling import ExistingTableHandling
from records_mover.db import DBDriver


class TestPrep(unittest.TestCase):
    def setUp(self):
        self.mock_tbl = Mock(name='target_table_details')
        self.prep = TablePrep(self.mock_tbl)

    @patch('records_mover.records.prep.quote_schema_and_table')
    def test_prep_table_exists_append_implicit(self, mock_quote_schema_and_table):
        mock_schema_sql = 'mock_schema_sql'
        mock_driver = Mock(name='driver', spec=DBDriver)
        mock_driver.db_conn = MagicMock(name='db')
        mock_driver.db_engine = MagicMock(name='db_engine')

        mock_driver.has_table.return_value = True
        how_to_prep = ExistingTableHandling.APPEND
        self.mock_tbl.existing_table_handling = how_to_prep

        self.prep.prep(mock_schema_sql, mock_driver)

    @patch('records_mover.records.prep.quote_schema_and_table')
    def test_prep_table_exists_truncate_implicit(self, mock_quote_schema_and_table):
        mock_schema_sql = 'mock_schema_sql'
        mock_driver = Mock(name='driver', spec=DBDriver)
        mock_driver.db_conn = MagicMock(name='db')
        mock_driver.db_engine = MagicMock(name='db_engine')

        mock_quote_schema_and_table
        mock_driver.has_table.return_value = True
        how_to_prep = ExistingTableHandling.TRUNCATE_AND_OVERWRITE
        mock_schema_and_table = mock_quote_schema_and_table.return_value
        self.mock_tbl.existing_table_handling = how_to_prep

        self.prep.prep(mock_schema_sql, mock_driver)

        mock_quote_schema_and_table.assert_called_with(None,
                                                       self.mock_tbl.schema_name,
                                                       self.mock_tbl.table_name,
                                                       db_engine=mock_driver.db_engine)
        str_arg = str(mock_driver.db_conn.execute.call_args.args[0])
        self.assertEqual(str_arg, f"TRUNCATE TABLE {mock_schema_and_table}")

    @patch('records_mover.records.prep.quote_schema_and_table')
    def test_prep_table_exists_delete_implicit(self, mock_quote_schema_and_table):
        mock_schema_sql = 'mock_schema_sql'
        mock_driver = Mock(name='driver', spec=DBDriver)
        mock_driver.db_conn = MagicMock(name='db')
        mock_driver.db_engine = MagicMock(name='db_engine')

        mock_quote_schema_and_table
        mock_driver.has_table.return_value = True
        how_to_prep = ExistingTableHandling.DELETE_AND_OVERWRITE
        mock_schema_and_table = mock_quote_schema_and_table.return_value
        self.mock_tbl.existing_table_handling = how_to_prep

        self.prep.prep(mock_schema_sql, mock_driver)

        mock_quote_schema_and_table.assert_called_with(None,
                                                       self.mock_tbl.schema_name,
                                                       self.mock_tbl.table_name,
                                                       db_engine=mock_driver.db_engine)
        str_arg = str(mock_driver.db_conn.execute.call_args.args[0])
        self.assertEqual(str_arg, f"DELETE FROM {mock_schema_and_table} WHERE true")

    @patch('records_mover.records.prep.quote_schema_and_table')
    def test_prep_table_exists_drop_implicit(self, mock_quote_schema_and_table):
        mock_schema_sql = 'mock_schema_sql'
        mock_driver = Mock(name='driver', spec=DBDriver)
        mock_driver.db_engine = mock_driver
        mock_db = MagicMock(name='db')
        mock_driver.db_conn = mock_db
        mock_driver.db_engine = MagicMock(name='db_engine')

        mock_quote_schema_and_table
        mock_driver.has_table.return_value = True
        how_to_prep = ExistingTableHandling.DROP_AND_RECREATE
        mock_schema_and_table = mock_quote_schema_and_table.return_value
        self.mock_tbl.existing_table_handling = how_to_prep

        self.prep.prep(mock_schema_sql, mock_driver)
        mock_conn = mock_driver.db_engine.connect.return_value.__enter__.return_value

        mock_quote_schema_and_table.assert_called_with(None,
                                                       self.mock_tbl.schema_name,
                                                       self.mock_tbl.table_name,
                                                       db_engine=mock_driver.db_engine)
        str_args = [str(call_arg.args[0]) for call_arg in mock_conn.execute.call_args_list]
        drop_table_str_arg, mock_schema_sql_str_arg = str_args[0], str_args[1]
        self.assertEqual(drop_table_str_arg, f"DROP TABLE {mock_schema_and_table}")
        self.assertEqual(mock_schema_sql_str_arg, mock_schema_sql)
        mock_driver.set_grant_permissions_for_groups.\
            assert_called_with(self.mock_tbl.schema_name,
                               self.mock_tbl.table_name,
                               self.mock_tbl.add_group_perms_for,
                               None,
                               db_conn=mock_conn)
        mock_driver.set_grant_permissions_for_users.\
            assert_called_with(self.mock_tbl.schema_name,
                               self.mock_tbl.table_name,
                               self.mock_tbl.add_user_perms_for,
                               None,
                               db_conn=mock_conn)

    @patch('records_mover.records.prep.quote_schema_and_table')
    def test_prep_table_not_exists(self, mock_quote_schema_and_table):
        mock_schema_sql = 'mock_schema_sql'
        mock_driver = Mock(name='driver', spec=DBDriver)
        mock_driver.db_engine = mock_driver
        mock_db = MagicMock(name='db')
        mock_driver.db_conn = mock_db
        mock_driver.db_engine = MagicMock(name='db_engine')

        mock_quote_schema_and_table
        mock_driver.has_table.return_value = False
        how_to_prep = ExistingTableHandling.DROP_AND_RECREATE
        self.mock_tbl.existing_table_handling = how_to_prep

        self.prep.prep(mock_schema_sql, mock_driver)
        mock_conn = mock_driver.db_engine.connect.return_value.__enter__.return_value
        print(mock_conn.execute)
        str_args = [str(call_arg.args[0]) for call_arg in mock_conn.execute.call_args_list]
        mock_schema_sql_str_arg = str_args[0]
        self.assertEqual(mock_schema_sql_str_arg, mock_schema_sql)
        mock_driver.set_grant_permissions_for_groups.\
            assert_called_with(self.mock_tbl.schema_name,
                               self.mock_tbl.table_name,
                               self.mock_tbl.add_group_perms_for,
                               None,
                               db_conn=mock_conn)
        mock_driver.set_grant_permissions_for_users.\
            assert_called_with(self.mock_tbl.schema_name,
                               self.mock_tbl.table_name,
                               self.mock_tbl.add_user_perms_for,
                               None,
                               db_conn=mock_conn)

    @patch('records_mover.records.prep.quote_schema_and_table')
    def test_prep_table_exists_drop_explicit(self, mock_quote_schema_and_table):
        mock_schema_sql = 'mock_schema_sql'
        mock_driver = Mock(name='driver', spec=DBDriver)
        mock_db = MagicMock(name='db')
        mock_driver.db_conn = mock_db
        mock_db_engine = MagicMock(name='db_engine')
        mock_driver.db_engine = mock_db_engine

        mock_quote_schema_and_table
        mock_driver.has_table.return_value = True
        how_to_prep = ExistingTableHandling.DELETE_AND_OVERWRITE
        mock_schema_and_table = mock_quote_schema_and_table.return_value
        self.mock_tbl.existing_table_handling = how_to_prep

        self.prep.prep(mock_schema_sql, mock_driver,
                       existing_table_handling=ExistingTableHandling.DROP_AND_RECREATE)
        mock_conn = mock_db_engine.connect.return_value.__enter__.return_value
        mock_quote_schema_and_table.assert_called_with(None,
                                                       self.mock_tbl.schema_name,
                                                       self.mock_tbl.table_name,
                                                       db_engine=mock_driver.db_engine)
        str_args = [str(call_arg.args[0]) for call_arg in mock_conn.execute.call_args_list]
        drop_table_str_arg, mock_schema_sql_str_arg = str_args[0], str_args[1]
        self.assertEqual(drop_table_str_arg, f"DROP TABLE {mock_schema_and_table}")
        self.assertEqual(mock_schema_sql_str_arg, mock_schema_sql)
        mock_driver.set_grant_permissions_for_groups.\
            assert_called_with(self.mock_tbl.schema_name,
                               self.mock_tbl.table_name,
                               self.mock_tbl.add_group_perms_for,
                               None,
                               db_conn=mock_conn)
        mock_driver.set_grant_permissions_for_users.\
            assert_called_with(self.mock_tbl.schema_name,
                               self.mock_tbl.table_name,
                               self.mock_tbl.add_user_perms_for,
                               None,
                               db_conn=mock_conn)
