from unittest.mock import MagicMock, patch, Mock
import unittest
from records_mover.records import DelimitedRecordsFormat
from records_mover.db.mysql.load_options import MySqlLoadOptions


class TestMySQLLoadOptions(unittest.TestCase):
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
        """
        self.assertEqual(sql, expected_sql)
