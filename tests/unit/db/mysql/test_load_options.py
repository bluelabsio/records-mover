import unittest
from records_mover.records import DelimitedRecordsFormat
from records_mover.db.mysql.load_options import MySqlLoadOptions, mysql_load_options


class TestMySQLLoadOptions(unittest.TestCase):
    maxDiff = None

    def test_generate_load_data_sql_boring(self) -> None:
        options = MySqlLoadOptions(character_set="utf8",
                                   fields_terminated_by="\t",
                                   fields_enclosed_by='',
                                   fields_optionally_enclosed_by=None,
                                   fields_escaped_by='\\',
                                   lines_starting_by='',
                                   lines_terminated_by='\n',
                                   ignore_n_lines=0)
        sql = options.generate_load_data_sql(filename="my_filename.txt",
                                             schema_name='myschema',
                                             table_name='mytable')
        expected_sql = """\
LOAD DATA
LOCAL INFILE 'my_filename.txt'
INTO TABLE myschema.mytable
CHARACTER SET 'utf8'
FIELDS
    TERMINATED BY '\\t'
    ENCLOSED BY ''
    ESCAPED BY '\\\\'
LINES
    STARTING BY ''
    TERMINATED BY '\\n'
IGNORE 0 LINES
"""
        sqltext = str(sql.compile(compile_kwargs={"literal_binds": True}))

        self.assertEqual(sqltext, expected_sql)

    def test_generate_load_data_sql_different_constants(self) -> None:
        options = MySqlLoadOptions(character_set="utf16",
                                   fields_terminated_by=",",
                                   fields_enclosed_by='"',
                                   fields_optionally_enclosed_by=None,
                                   fields_escaped_by='\\',
                                   lines_starting_by='abc',
                                   lines_terminated_by='\r\n',
                                   ignore_n_lines=1)
        sql = options.generate_load_data_sql(filename="another_filename.txt",
                                             schema_name='myschema',
                                             table_name='mytable')
        expected_sql = """\
LOAD DATA
LOCAL INFILE 'another_filename.txt'
INTO TABLE myschema.mytable
CHARACTER SET 'utf16'
FIELDS
    TERMINATED BY ','
    ENCLOSED BY '"'
    ESCAPED BY '\\\\'
LINES
    STARTING BY 'abc'
    TERMINATED BY '\\r\\n'
IGNORE 1 LINES
"""
        sqltext = str(sql.compile(compile_kwargs={"literal_binds": True}))

        self.assertEqual(sqltext, expected_sql)

    def test_generate_load_data_sql_field_enclosed_by_None(self) -> None:
        options = MySqlLoadOptions(character_set="utf16",
                                   fields_terminated_by=",",
                                   fields_enclosed_by=None,
                                   fields_optionally_enclosed_by=None,
                                   fields_escaped_by='\\',
                                   lines_starting_by='abc',
                                   lines_terminated_by='\r\n',
                                   ignore_n_lines=1)
        sql = options.generate_load_data_sql(filename="another_filename.txt",
                                             schema_name='myschema',
                                             table_name='mytable')
        expected_sql = """\
LOAD DATA
LOCAL INFILE 'another_filename.txt'
INTO TABLE myschema.mytable
CHARACTER SET 'utf16'
FIELDS
    TERMINATED BY ','
    ESCAPED BY '\\\\'
LINES
    STARTING BY 'abc'
    TERMINATED BY '\\r\\n'
IGNORE 1 LINES
"""
        sqltext = str(sql.compile(compile_kwargs={"literal_binds": True}))

        self.assertEqual(sqltext, expected_sql)

    def test_generate_load_data_sql_field_optionally_enclosed_by(self) -> None:
        options = MySqlLoadOptions(character_set="utf16",
                                   fields_terminated_by=",",
                                   fields_enclosed_by=None,
                                   fields_optionally_enclosed_by='"',
                                   fields_escaped_by='\\',
                                   lines_starting_by='abc',
                                   lines_terminated_by='\r\n',
                                   ignore_n_lines=1)
        sql = options.generate_load_data_sql(filename="another_filename.txt",
                                             schema_name='myschema',
                                             table_name='mytable')
        expected_sql = """\
LOAD DATA
LOCAL INFILE 'another_filename.txt'
INTO TABLE myschema.mytable
CHARACTER SET 'utf16'
FIELDS
    TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    ESCAPED BY '\\\\'
LINES
    STARTING BY 'abc'
    TERMINATED BY '\\r\\n'
IGNORE 1 LINES
"""
        sqltext = str(sql.compile(compile_kwargs={"literal_binds": True}))

        self.assertEqual(sqltext, expected_sql)

    def test_generate_load_data_sql_field_optionally_enclosed_by_and_enclosed_by_set(self) -> None:
        with self.assertRaises(SyntaxError):
            options = MySqlLoadOptions(character_set="utf16",
                                       fields_terminated_by=",",
                                       fields_enclosed_by='"',
                                       fields_optionally_enclosed_by='"',
                                       fields_escaped_by='\\',
                                       lines_starting_by='abc',
                                       lines_terminated_by='\r\n',
                                       ignore_n_lines=1)
            options.generate_load_data_sql(filename="another_filename.txt",
                                           schema_name='myschema',
                                           table_name='mytable')

    def test_generate_load_data_sql_windows_filename(self) -> None:
        options = MySqlLoadOptions(character_set="utf16",
                                   fields_terminated_by=",",
                                   fields_enclosed_by=None,
                                   fields_optionally_enclosed_by='"',
                                   fields_escaped_by='\\',
                                   lines_starting_by='abc',
                                   lines_terminated_by='\r\n',
                                   ignore_n_lines=1)
        sql = options.generate_load_data_sql("c:\\Some Path\\OH GOD LET IT END~1.CSV",
                                             schema_name='myschema',
                                             table_name='mytable')
        # https://dev.mysql.com/doc/refman/8.0/en/load-data.html
        #
        # The file name must be given as a literal string. On Windows,
        # specify backslashes in path names as forward slashes or
        # doubled backslashes.
        #
        expected_sql = """\
LOAD DATA
LOCAL INFILE 'c:\\\\Some Path\\\\OH GOD LET IT END~1.CSV'
INTO TABLE myschema.mytable
CHARACTER SET 'utf16'
FIELDS
    TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    ESCAPED BY '\\\\'
LINES
    STARTING BY 'abc'
    TERMINATED BY '\\r\\n'
IGNORE 1 LINES
"""
        sqltext = str(sql.compile(compile_kwargs={"literal_binds": True}))

        self.assertEqual(sqltext, expected_sql)

    def test_vertica_dialect_style_terminators(self) -> None:
        options = MySqlLoadOptions(character_set="utf16",
                                   fields_terminated_by="\002",
                                   fields_enclosed_by=None,
                                   fields_optionally_enclosed_by='"',
                                   fields_escaped_by='\\',
                                   lines_starting_by='abc',
                                   lines_terminated_by="\001",
                                   ignore_n_lines=1)
        sql = options.generate_load_data_sql(filename="another_filename.txt",
                                             schema_name='myschema',
                                             table_name='mytable')
        # TODO: Verify that these work
        expected_sql = """\
LOAD DATA
LOCAL INFILE 'another_filename.txt'
INTO TABLE myschema.mytable
CHARACTER SET 'utf16'
FIELDS
    TERMINATED BY '\\x02'
    OPTIONALLY ENCLOSED BY '"'
    ESCAPED BY '\\\\'
LINES
    STARTING BY 'abc'
    TERMINATED BY '\\x01'
IGNORE 1 LINES
"""
        sqltext = str(sql.compile(compile_kwargs={"literal_binds": True}))

        self.assertEqual(sqltext, expected_sql)

    def test_mysql_load_options_encoding_utf8bom_fail(self) -> None:
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={
                                                    'encoding': 'UTF8BOM',
                                                    'compression': None,
                                                })
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaises(NotImplementedError) as r:
            mysql_load_options(unhandled_hints,
                               records_format,
                               fail_if_cant_handle_hint=True)
        self.assertIn('UTF8BOM', str(r.exception))

    def test_mysql_load_options_encoding_utf8bom_fallback(self) -> None:
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={
                                                    'encoding': 'UTF8BOM',
                                                    'compression': None,
                                                })
        unhandled_hints = set(records_format.hints.keys())
        out = mysql_load_options(unhandled_hints,
                                 records_format,
                                 fail_if_cant_handle_hint=False)
        self.assertEqual(out.character_set, 'utf8')

    def test_mysql_load_options_all_quoting(self) -> None:
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={
                                                    'quoting': 'all',
                                                    'doublequote': True,
                                                    'compression': None,
                                                })
        unhandled_hints = set(records_format.hints.keys())
        out = mysql_load_options(unhandled_hints,
                                 records_format,
                                 fail_if_cant_handle_hint=True)
        self.assertEqual(out.fields_enclosed_by, '"')

    def test_mysql_load_options_nonnumeric_quoting(self) -> None:
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={
                                                    'quoting': 'nonnumeric',
                                                    'doublequote': True,
                                                    'compression': None,
                                                })
        unhandled_hints = set(records_format.hints.keys())
        out = mysql_load_options(unhandled_hints,
                                 records_format,
                                 fail_if_cant_handle_hint=True)
        self.assertEqual(out.fields_optionally_enclosed_by, '"')

    def test_mysql_load_options_bogus_quoting(self) -> None:
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={
                                                    'quoting': 'no thanks',
                                                    'doublequote': True,
                                                    'compression': None,
                                                })
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaises(NotImplementedError) as r:
            mysql_load_options(unhandled_hints,
                               records_format,
                               fail_if_cant_handle_hint=True)

        self.assertIn('no thanks', str(r.exception))

    def test_mysql_load_options_valid_quoting_no_doublequote(self) -> None:
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={
                                                    'quoting': 'all',
                                                    'doublequote': False,
                                                    'compression': None,
                                                })
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaises(NotImplementedError) as r:
            mysql_load_options(unhandled_hints,
                               records_format,
                               fail_if_cant_handle_hint=True)

        self.assertIn('doublequote=False', str(r.exception))

    def test_mysql_load_options_valid_quoting_bogus_doublequote(self) -> None:
        records_format = DelimitedRecordsFormat(variant='bluelabs',
                                                hints={
                                                    'quoting': 'all',
                                                    'doublequote': 'mumble',
                                                    'compression': None,
                                                })
        unhandled_hints = set(records_format.hints.keys())
        with self.assertRaises(NotImplementedError) as r:
            mysql_load_options(unhandled_hints,
                               records_format,
                               fail_if_cant_handle_hint=True)

        self.assertIn("doublequote='mumble'", str(r.exception))
