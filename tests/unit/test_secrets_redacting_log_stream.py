from records_mover.logging import register_secret, SecretsRedactingLogStream
import io
import unittest


class TestSecretsRedactingLogStream(unittest.TestCase):
    def test_redacts(self):
        register_secret("s3krit")
        logging_input = "foo\ns3krit\nbaz"
        expected_output = "foo\n******\nbaz"
        output_stream = io.StringIO()

        log_stream = SecretsRedactingLogStream(output_stream)
        log_stream.write(logging_input)
        self.assertEqual(output_stream.getvalue(), expected_output)
