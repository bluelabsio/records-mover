import unittest
from mock import Mock, patch
from records_mover.db.vertica.import_sql import vertica_import_sql
import re  # now I have two problems


def normalize_whitespace(s):
    return re.sub(r"\s+", " ", s)


@patch('records_mover.db.vertica.import_sql.quote_schema_and_table')
@patch('records_mover.db.vertica.import_sql.quote_value')
class TestImportSQL(unittest.TestCase):
    maxDiff = None

    def test_vertica_import_sql(self,
                                mock_quote_value,
                                mock_quote_schema_and_table):
        def null_schema_table_quoter(db, schema, table, db_engine=None):
            return f"{schema}.{table}"

        mock_quote_schema_and_table.side_effect = null_schema_table_quoter

        def simple_value_quoter(db, s, db_engine=None):
            return f"'{s}'"

        mock_quote_value.side_effect = simple_value_quoter

        mock_db_engine = Mock(name='db_engine')
        expected = """
            COPY myschema.mytable
            FROM STDIN
            GZIP
            DELIMITER AS E','

            NULL AS 'X'
            NO ESCAPE

            RECORD TERMINATOR E'F'
            SKIP 0


            REJECTMAX 123


            ENFORCELENGTH

            ABORT ON ERROR
            STORAGE 123

            NO COMMIT
            ;

        """
        out = vertica_import_sql(schema='myschema',
                                 table='mytable',
                                 db_engine=mock_db_engine,
                                 gzip=True,
                                 delimiter=',',
                                 trailing_nullcols=False,
                                 record_terminator='F',
                                 null_as='X',
                                 rejectmax=123,
                                 enforcelength=True,
                                 error_tolerance=False,
                                 abort_on_error=True,
                                 load_method='123',
                                 no_commit=True)
        self.assertEqual(normalize_whitespace(out), normalize_whitespace(expected))
