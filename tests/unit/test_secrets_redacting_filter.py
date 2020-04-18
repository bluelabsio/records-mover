from records_mover.logging import register_secret, SecretsRedactingFilter
import logging
import unittest


class TestSecretsRedactingFilter(unittest.TestCase):
    def test_redacts(self):
        register_secret("s3krit")
        logging_input = "foo\ns3krit\nbaz"
        expected_output = "foo\n******\nbaz"
        log_record = logging.LogRecord(name='name',
                                       level=logging.INFO,
                                       pathname='pathname',
                                       lineno=123,
                                       msg=logging_input,
                                       args=[],
                                       exc_info=None,
                                       func='func',
                                       sinfo=None)
        log_filter = SecretsRedactingFilter()
        out = log_filter.filter(log_record)

        self.assertEqual(out, True)
        self.assertEqual(log_record.msg, expected_output)
