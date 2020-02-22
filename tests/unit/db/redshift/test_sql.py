import unittest
import sqlalchemy
from records_mover.db.redshift.sql import schema_sql_from_admin_views
from records_mover.db.redshift.unloader import RedshiftUnloader
from records_mover.records.records_format import DelimitedRecordsFormat
from mock import patch, Mock


class TestSQL(unittest.TestCase):
    @patch('records_mover.db.redshift.sql.logger')
    def test_schema_sql_from_admin_views_not_installed(self,
                                                       mock_logger):
        mock_db = Mock(name='db')
        mock_table = Mock(name='table')
        mock_schema = Mock(name='schema')
        mock_db.execute.side_effect = sqlalchemy.exc.ProgrammingError('statement', {}, {})
        out = schema_sql_from_admin_views(mock_schema, mock_table, mock_db)
        self.assertIsNone(out)
        mock_logger.debug.assert_called_with('Error while generating SQL', exc_info=True)
        mock_logger.warning.\
            assert_called_with("To be able to save SQL to a records directory, "
                               "please install and grant access to 'admin.v_generate_tbl_ddl' "
                               "from https://github.com/awslabs/amazon-redshift-utils/"
                               "blob/master/src/AdminViews/v_generate_tbl_ddl.sql")
