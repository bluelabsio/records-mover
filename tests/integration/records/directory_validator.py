import json
import logging
import gzip
import os
import urllib
import subprocess
import jsonschema
from subprocess import CalledProcessError

logger = logging.getLogger(__name__)


class RecordsDirectoryValidator:
    def __init__(self, records_dir, test_name):
        self.records_dir = records_dir
        self.test_name = test_name
        self.concatenated_file = f"{self.records_dir}/_concatenated_file"
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(f"{dir_path}/records_schema_v1_schema.json") as records_schema_schema_data:
            self.records_schema_schema = json.load(records_schema_schema_data)

    def assert_records_file_exists(self, filename):
        with open(f"{self.records_dir}/{filename}"):
            pass  # open will raise if file doesn't exist

    def assert_records_file_not_exists(self, filename):
        try:
            with open(f"{self.records_dir}/{filename}"):
                assert False, f"{filename} not expected to exist!"
        except FileNotFoundError:
            return

    def validate_records_schema(self):
        schema_file = f"{self.records_dir}/_schema.json"
        with open(schema_file) as records_schema_data:
            records_schema = json.load(records_schema_data)
            try:
                jsonschema.validate(records_schema, schema=self.records_schema_schema)
            except jsonschema.exceptions.ValidationError as e:
                print(f"Error while loading {e.absolute_path} in {schema_file}:")
                print(json.dumps(records_schema, indent=4, sort_keys=True))
                raise

            actual_field_names = list(records_schema['fields'].keys())
            expected_field_names = ['num', 'numstr', 'str', 'comma',
                                    'doublequote', 'quotecommaquote',
                                    'newlinestr', 'date', 'time',
                                    'timestamp', 'timestamptz']
            assert actual_field_names == expected_field_names, actual_field_names
            actual_field_types = [field['type'] for field in records_schema['fields'].values()]
            acceptable_field_types = [
                # gold star if you can do all of this
                [
                    'integer', 'string', 'string', 'string',
                    'string', 'string', 'string', 'date', 'time',
                    'datetime', 'datetimetz'
                ],
                # Redshift doesn't support TIME type:
                # https://docs.aws.amazon.com/redshift/latest/dg/r_Datetime_types.html
                [
                    'integer', 'string', 'string', 'string',
                    'string', 'string', 'string', 'date', 'string',
                    'datetime', 'datetimetz'
                ],
                # MySQL's datetimetz type ("TIMESTAMP") doesn't
                # support dates before the Unix epoch (Jan 1 1970),
                # and records-mover does not yet support using
                # inference to determine if the data in question will
                # fit into it.
                #
                # https://app.asana.com/0/1128138765527694/1166526213569051
                # https://stackoverflow.com/questions/31761047/what-difference-between-the-date-time-datetime-and-timestamp-types/56138746
                [
                    'integer', 'string', 'string', 'string',
                    'string', 'string', 'string', 'date', 'time',
                    'datetime', 'datetime'
                ],
            ]
            assert actual_field_types in acceptable_field_types,\
                f"\nreceived {actual_field_types}, \nexpected {acceptable_field_types}"

    def validate(self):
        self.assert_records_file_exists('_manifest')
        self.assert_records_file_exists('_format_delimited')
        self.validate_records_schema()
        with open(f"{self.records_dir}/_manifest") as manifest_data, \
                open(f"{self.records_dir}/_format_delimited") as format_data:
            manifest = json.load(manifest_data)
            format = json.load(format_data)
            logger.info(f"manifest: {manifest}")
            logger.info(f"format: {format}")
            entries = manifest['entries']
            assert len(entries) > 0
            with open(self.concatenated_file, "wb") as whole:
                for url in entries:
                    loc = urllib.parse.urlparse(url['url'])
                    logger.info(f"loc: {loc}")
                    filename = os.path.basename(loc.path)
                    self.assert_records_file_exists(filename)
                    with open(f"{self.records_dir}/{filename}", "rb") as part:
                        compression = format['hints']['compression']
                        if compression == 'GZIP':
                            decompressed = gzip.decompress(part.read())
                            whole.write(decompressed)
                        elif compression is None:
                            whole.write(part.read())
                        else:
                            err = f"Teach me how to decode {format['compression']}"
                            raise NotImplementedError(err)
                    logger.info(f"verified {filename}")
        dir_path = os.path.dirname(os.path.realpath(__file__))

        outputs = {}
        success = False

        for alt in ['', '-pandas', '-pandas-utc', '-utc', '-pandas-notz']:
            expected_file = f"{dir_path}/../resources/{self.test_name}{alt}.csv"
            logger.info(f"expected_file: {expected_file}")
            try:
                subprocess.check_output(["diff", "-u", self.concatenated_file, expected_file],
                                        stderr=subprocess.STDOUT)
                success = True
            except CalledProcessError as e:
                outputs[expected_file] = e.output.decode('utf-8')

        formatted_output = '\n\n\n'.join(outputs.values())
        assert success, f"File did not match:\n\n {formatted_output}"
