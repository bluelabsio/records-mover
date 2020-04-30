from unittest.mock import MagicMock, patch, Mock
import unittest
from records_mover.records import DelimitedRecordsFormat
from records_mover.db.mysql.load_options import MySqlLoadOptions


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
        sql = options.generate_load_data_sql("my_filename.txt")
        expected_sql = """\
LOAD DATA
LOCAL INFILE 'my_filename.txt'
CHARACTER SET 'utf8'
FIELDS
    TERMINATED_BY '\\t'
    ENCLOSED_BY ''
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
        sql = options.generate_load_data_sql("another_filename.txt")
        expected_sql = """\
LOAD DATA
LOCAL INFILE 'another_filename.txt'
CHARACTER SET 'utf16'
FIELDS
    TERMINATED_BY ','
    ENCLOSED_BY '"'
    ESCAPED BY '\\\\'
LINES
    STARTING BY 'abc'
    TERMINATED BY '\\r\\n'
IGNORE 1 LINES
"""
        sqltext = str(sql.compile(compile_kwargs={"literal_binds": True}))

        self.assertEqual(sqltext, expected_sql)

    def test_generate_load_data_sql_field_enlosed_by_None(self) -> None:
        options = MySqlLoadOptions(character_set="utf16",
                                   fields_terminated_by=",",
                                   fields_enclosed_by=None,
                                   fields_optionally_enclosed_by=None,
                                   fields_escaped_by='\\',
                                   lines_starting_by='abc',
                                   lines_terminated_by='\r\n',
                                   ignore_n_lines=1)
        sql = options.generate_load_data_sql("another_filename.txt")
        expected_sql = """\
LOAD DATA
LOCAL INFILE 'another_filename.txt'
CHARACTER SET 'utf16'
FIELDS
    TERMINATED_BY ','
    ESCAPED BY '\\\\'
LINES
    STARTING BY 'abc'
    TERMINATED BY '\\r\\n'
IGNORE 1 LINES
"""
        sqltext = str(sql.compile(compile_kwargs={"literal_binds": True}))

        self.assertEqual(sqltext, expected_sql)
