from unittest.mock import MagicMock, patch, Mock
import unittest
from records_mover.records import DelimitedRecordsFormat
from records_mover.db.mysql.load_options import MySqlLoadOptions


class TestMySQLLoadOptions(unittest.TestCase):
    maxDiff = None

    def test_generate_load_data_sql_boring(self) -> None:
        options = MySqlLoadOptions(character_set="utf8",
                                   field_terminator="\t",
                                   field_enclosed_by='',
                                   field_optionally_enclosed_by=None,
                                   field_escaped_by='\\',
                                   line_starting_by='',
                                   line_terminated_by='\n',
                                   ignore_n_lines=0)
        sql = options.generate_load_data_sql("my_filename.txt")
        expected_sql = """
        LOAD DATA
        LOCAL INFILE 'my_filename.txt'
        CHARACTER SET 'utf8'
        FIELDS
            TERMINATED_BY '\\t'
            ENCLOSED_BY ''
            ESCAPED BY '\\'
        LINES
            STARTING BY ''
            TERMINATED BY '\\n'
        IGNORE 0 LINES
        """
        self.assertEqual(str(sql), expected_sql)

    def test_generate_load_data_sql_different_constants(self) -> None:
        options = MySqlLoadOptions(character_set="utf16",
                                   field_terminator=",",
                                   field_enclosed_by='"',
                                   field_optionally_enclosed_by=None,
                                   field_escaped_by='\\',
                                   line_starting_by='',
                                   line_terminated_by='\r\n',
                                   ignore_n_lines=1)
        sql = options.generate_load_data_sql("another_filename.txt")
        expected_sql = """
        LOAD DATA
        LOCAL INFILE 'another_filename.txt'
        CHARACTER SET 'utf16'
        FIELDS
            TERMINATED_BY ','
            ENCLOSED_BY '"'
            ESCAPED BY '\\'
        LINES
            STARTING BY 'abc'
            TERMINATED BY '\r\n'
        IGNORE 1 LINES
        """
        self.assertEqual(str(sql), expected_sql)
