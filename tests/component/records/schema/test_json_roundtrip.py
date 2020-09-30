import unittest
import json
from records_mover.records.schema import RecordsSchema, UnsupportedSchemaError
import os


class TestRecordsSchemaJsonRoundtrip(unittest.TestCase):
    maxDiff = None

    def test_json_roundtrip_redshift_v1(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        sample_filename = os.path.join(dir_path, 'redshift_example_1.json')
        with open(sample_filename) as f:
            sample_str = f.read()
        records_schema = RecordsSchema.from_json(sample_str)
        sample_data = json.loads(sample_str)
        output_str = records_schema.to_json()
        output_data = json.loads(output_str)
        self.assertDictEqual(sample_data, output_data)

    def test_json_roundtrip_pandas_v1(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        sample_filename = os.path.join(dir_path, 'pandas_example_1.json')
        with open(sample_filename) as f:
            sample_str = f.read()
        records_schema = RecordsSchema.from_json(sample_str)
        sample_data = json.loads(sample_str)
        output_str = records_schema.to_json()
        output_data = json.loads(output_str)
        self.assertDictEqual(sample_data, output_data)

    def test_json_roundtrip_future_incompatible_format(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        sample_filename = os.path.join(dir_path, 'future_incompatible_redshift_example_1.json')
        with open(sample_filename) as f:
            sample_str = f.read()
        with self.assertRaises(UnsupportedSchemaError):
            RecordsSchema.from_json(sample_str)
