import unittest
import sqlalchemy
from records_mover.db.redshift.sql import schema_sql_from_admin_views
from mock import patch, Mock, MagicMock


class TestSQL(unittest.TestCase):
    @patch('records_mover.db.redshift.sql.logger')
    def test_schema_sql_from_admin_views_not_installed(self,
                                                       mock_logger):
        mock_db = MagicMock(name='db')
        mock_table = Mock(name='table')
        mock_schema = Mock(name='schema')
        mock_connection = MagicMock(name='connection')
        mock_db.connect.return_value \
               .__enter__.return_value = mock_connection
        mock_connection.begin.return_value \
                       .__enter__.return_value = None
        mock_connection.execute.side_effect = sqlalchemy.exc.ProgrammingError('statement', {}, {})
        out = schema_sql_from_admin_views(mock_schema, mock_table, None, db_conn=mock_connection)
        self.assertIsNone(out)
        mock_logger.debug.assert_called_with('Error while generating SQL', exc_info=True)
        mock_logger.warning.\
            assert_called_with("To be able to save SQL to a records directory, "
                               "please install and grant access to 'admin.v_generate_tbl_ddl' "
                               "from https://github.com/awslabs/amazon-redshift-utils/"
                               "blob/master/src/AdminViews/v_generate_tbl_ddl.sql")
