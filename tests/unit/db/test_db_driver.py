from .fakes import fake_text
import unittest
from mock import Mock, MagicMock
from records_mover.db.driver import GenericDBDriver
import sqlalchemy


class TestDBDriver(unittest.TestCase):
    def setUp(self):
        self.mock_db_engine = MagicMock(name='db_engine')
        self.mock_url_resolver = Mock(name='url_resolver')
        self.mock_s3_temp_base_loc = MagicMock(name='s3_temp_base_loc')
        self.mock_s3_temp_base_loc.url = 's3://fakebucket/fakedir/fakesubdir/'
        self.db_driver = GenericDBDriver(db=None,
                                         db_engine=self.mock_db_engine,
                                         s3_temp_base_loc=self.mock_s3_temp_base_loc,
                                         url_resolver=self.mock_url_resolver,
                                         text=fake_text)

    def test_table(self):
        out = self.db_driver.table('my_schema', 'my_table')
        self.assertEqual(out.name, 'my_table')
        self.assertEqual(out.schema, 'my_schema')

    def test_supports_time_type(self):
        out = self.db_driver.supports_time_type()
        self.assertEqual(out, True)

    def test_varchar_length_is_in_chars(self):
        out = self.db_driver.varchar_length_is_in_chars()
        self.assertEqual(out, False)

    def test_type_for_date_plus_time(self):
        mock_has_tz = Mock(name='has_tz')
        out = self.db_driver.type_for_date_plus_time(has_tz=mock_has_tz)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.DateTime)
        self.assertEqual(out.timezone, mock_has_tz)

    def test_type_for_integer(self):
        out = self.db_driver.type_for_integer(123, 456)
        self.assertEqual(type(out), sqlalchemy.sql.sqltypes.Integer)

    def test_make_column_name_valid(self):
        mock_colname = Mock(name='colname')
        out = self.db_driver.make_column_name_valid(mock_colname)
        self.assertEqual(out, mock_colname)

    def test_integer_limits(self):
        mock_type_ = Mock(name='type_')
        out = self.db_driver.integer_limits(mock_type_)
        self.assertIsNone(out)

    def test_fp_constraints(self):
        mock_type_ = Mock(name='type_')
        out = self.db_driver.fp_constraints(mock_type_)
        self.assertIsNone(out)

    def test_fixed_point_constraints(self):
        mock_type_ = Mock(name='type_')
        out = self.db_driver.fixed_point_constraints(mock_type_)
        self.assertEqual(out, (mock_type_.precision, mock_type_.scale))

    def test_fixed_point_constraints_no_precision(self):
        mock_type_ = Mock(name='type_')
        mock_type_.precision = None
        out = self.db_driver.fixed_point_constraints(mock_type_)
        self.assertEqual(out, None)

    def test_has_table(self):
        mock_schema = Mock(name='schema')
        mock_table = Mock(name='table')
        out = self.db_driver.has_table(mock_schema, mock_table)
        self.assertEqual(out, self.mock_db_engine._sa_instance_state.has_table.return_value)
        self.mock_db_engine._sa_instance_state.has_table.assert_called_with(mock_table,
                                                                            schema=mock_schema)

    def test_set_grant_permissions_for_groups(self):
        mock_schema_name = 'schema_name'
        mock_table = 'table'
        mock_db = Mock(name='db')
        groups = {
            'insert': ['group_a', 'group_b']
        }
        self.db_driver.set_grant_permissions_for_groups(mock_schema_name,
                                                        mock_table,
                                                        groups,
                                                        None,
                                                        db_conn=mock_db)
        str_args = [str(arg[0][0]) for arg in mock_db.execute.call_args_list]
        self.assertEqual([
            f"GRANT INSERT ON {mock_schema_name}.\"{mock_table}\" TO \"{groups['insert'][0]}\"\n",
            f"GRANT INSERT ON {mock_schema_name}.\"{mock_table}\" TO \"{groups['insert'][1]}\"\n",
        ], str_args)

    def test_set_grant_permissions_for_users(self):
        mock_schema_name = 'schema_name'
        mock_table = 'my_table'
        mock_db = Mock(name='db')
        users = {
            'insert': ['user_a', 'user_b']
        }
        self.db_driver.set_grant_permissions_for_users(mock_schema_name,
                                                       mock_table,
                                                       users,
                                                       None,
                                                       db_conn=mock_db)
        str_args = [str(arg[0][0]) for arg in mock_db.execute.call_args_list]
        self.assertEqual([
            f"GRANT INSERT ON {mock_schema_name}.{mock_table} TO \"{users['insert'][0]}\"\n",
            f"GRANT INSERT ON {mock_schema_name}.{mock_table} TO \"{users['insert'][1]}\"\n",
        ], str_args)

    def test_set_grant_permissions_for_users_bobby_tables(self):
        mock_schema_name = Mock(name='schema_name')
        mock_table = Mock(name='table')
        mock_db = Mock(name='db')
        users = {
            '; DESTROY ALL MY DATA MUHAHAHAH;': ['user_a', 'user_b']
        }
        with self.assertRaises(TypeError):
            self.db_driver.set_grant_permissions_for_users(mock_schema_name,
                                                           mock_table,
                                                           users,
                                                           None,
                                                           db_conn=mock_db)

    def test_set_grant_permissions_for_groups_bobby_tables(self):
        mock_schema_name = Mock(name='schema_name')
        mock_table = Mock(name='table')
        mock_db = Mock(name='db')
        groups = {
            '; DESTROY ALL MY DATA MUHAHAHAH;': ['group_a', 'group_b']
        }
        with self.assertRaises(TypeError):
            self.db_driver.set_grant_permissions_for_groups(mock_schema_name,
                                                            mock_table,
                                                            groups,
                                                            None,
                                                            db_conn=mock_db)

    def test_tweak_records_schema_for_load_no_tweak(self):
        mock_records_schema = Mock(name='records_schema')
        mock_records_format = Mock(name='records_format')
        self.assertEqual(mock_records_schema,
                         self.db_driver.tweak_records_schema_for_load(mock_records_schema,
                                                                      mock_records_format))
