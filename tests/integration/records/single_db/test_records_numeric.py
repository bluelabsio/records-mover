import logging
from .base_records_test import BaseRecordsIntegrationTest
import tempfile
import jsonschema
import os
import json
import pathlib
from .numeric_expectations import expected_field_info, expected_column_types
from records_mover.records.schema import RecordsSchema
from ..records_numeric_database_fixture import RecordsNumericDatabaseFixture

logger = logging.getLogger(__name__)

dir_path = os.path.dirname(os.path.realpath(__file__))
with open(f"{dir_path}/../records_schema_v1_schema.json") as records_schema_schema_data:
    records_schema_schema = json.load(records_schema_schema_data)

with open(f"{dir_path}/../../resources/example_numeric_records_schema.json")\
     as example_numeric_records_schema_file:
    example_numeric_records_schema = json.load(example_numeric_records_schema_file)


class RecordsNumericIntegrationTest(BaseRecordsIntegrationTest):
    def validate_records_schema(self, records_dir):
        schema_file = f"{records_dir}/_schema.json"
        with open(schema_file) as records_schema_data:
            records_schema = json.load(records_schema_data)
            try:
                jsonschema.validate(records_schema, schema=records_schema_schema)
            except jsonschema.exceptions.ValidationError as e:
                print(f"Error while loading {e.absolute_path} in {schema_file}:")
                print(json.dumps(records_schema, indent=4, sort_keys=True))
                raise

            for field_name, field in records_schema['fields'].items():
                expected_field_type = expected_field_info[field_name]['type']
                self.assertEqual(expected_field_type, field['type'],
                                 f"For {field_name}, expected {expected_field_type} and "
                                 f"got {field['type']}")
                expected_field_constraints = expected_field_info[field_name]['constraints'].copy()
                expected_field_constraints['required'] = False
                self.assertEqual(expected_field_constraints, field['constraints'],
                                 f"While reading {field_name}")

    def setUp(self):
        super().setUp()
        self.numeric_fixture = RecordsNumericDatabaseFixture(self.engine,
                                                             schema_name=self.schema_name,
                                                             table_name=self.table_name)
        self.numeric_fixture.tear_down()

    def test_numeric_schema_fields_created(self):
        self.numeric_fixture.bring_up()
        with tempfile.TemporaryDirectory(prefix='test_records_numeric_schema') as tempdir:
            output_url = pathlib.Path(tempdir).resolve().as_uri() + '/'
            records_format = self.records.RecordsFormat()
            processing_instructions = self.records.ProcessingInstructions()
            source = self.records.sources.table(schema_name=self.schema_name,
                                                table_name=self.table_name,
                                                db_engine=self.engine)
            target = self.records.targets.directory_from_url(output_url,
                                                             records_format=records_format)
            out = self.records.move(source, target, processing_instructions)
            self.assertIn(out.move_count, [1, None])
            self.validate_records_schema(tempdir)

    def validate_table(self):
        columns = self.engine.dialect.get_columns(self.engine, self.table_name,
                                                  schema=self.schema_name)
        # Note that Redshift doesn't support TIME type:
        # https://docs.aws.amazon.com/redshift/latest/dg/r_Datetime_types.html
        actual_column_types = {
            column['name']: str(column['type']) for column in columns
        }
        for colname in actual_column_types:
            assert actual_column_types[colname] ==\
                expected_column_types[self.engine.name][colname],\
                f"For {colname} on {self.engine.name}, " \
                f"expected {expected_column_types[self.engine.name][colname]}, "\
                f"got {actual_column_types[colname]}"

    def test_numeric_database_columns_created(self):
        records_schema = RecordsSchema.from_data(example_numeric_records_schema)
        processing_instructions = self.records.ProcessingInstructions()
        preferred_records_format = {
            'redshift': 'bluelabs',
            'bigquery': 'bigquery',
            'vertica': 'vertica',
            'postgresql': 'bluelabs',
        }
        source = self.records.sources.\
            local_file('/dev/null',
                       records_format=self.records.
                       RecordsFormat(format_type='delimited',
                                     variant=preferred_records_format[self.engine.name]),
                       records_schema=records_schema)
        target = self.records.targets.table(schema_name=self.schema_name,
                                            table_name=self.table_name,
                                            db_engine=self.engine)
        out = self.records.move(source, target, processing_instructions)
        self.assertIn(out.move_count, [0, None])
        self.validate_table()
