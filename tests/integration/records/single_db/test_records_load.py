import logging
from contextlib import contextmanager
from .base_records_test import BaseRecordsIntegrationTest
from ..table_validator import RecordsTableValidator

from records_mover.records import RecordsSchema, RecordsFormat

logger = logging.getLogger(__name__)


class RecordsLoadIntegrationTest(BaseRecordsIntegrationTest):
    def load_and_verify(self, format_type, variant, hints={}, broken=False, sourcefn=None):
        redshift_with_no_bucket = (self.engine.name == 'redshift' and
                                   not self.has_scratch_s3_bucket())
        if redshift_with_no_bucket:
            # https://github.com/bluelabsio/records-mover/issues/81
            logger.warning("This test won't pass until we can use the "
                           "records schema from the target to cast the "
                           "dataframe types appropriately, so skipping.")
            return
        if broken:
            expected_exception = 'sqlalchemy.exc.SQLAlchemyError'
            if self.engine.name == 'vertica':
                expected_exception = 'vertica_python.errors.CopyRejected'
            try:
                self.load(format_type, variant, hints, broken=broken)
                self.fail("No exception raised when loading bogus file")
            except Exception as e:
                self.assertTrue(str(e), expected_exception)
        else:
            self.load(format_type, variant, hints, broken=broken, sourcefn=sourcefn)
            self.verify_db_table(variant)

    def test_load_csv_format(self):
        self.load_and_verify('delimited', 'csv')

    def test_load_bigquery_format(self):
        self.load_and_verify('delimited', 'bigquery')

    def test_load_bigquery_format_with_header_row(self):
        self.load_and_verify('delimited', 'bigquery', {'header-row': True})

    def test_load_bluelabs_format(self):
        self.load_and_verify('delimited', 'bluelabs')

    def test_load_bluelabs_format_with_header_row(self):
        self.load_and_verify('delimited', 'bluelabs', {'header-row': True})

    def test_load_error(self):
        self.load_and_verify('delimited', 'bluelabs', hints={'compression': None}, broken=True)

    def test_load_from_s3_records_directory(self):
        try:
            import boto3  # noqa
        except ModuleNotFoundError:
            logger.warning("Not running in test environment with boto3, "
                           "so skipping records directory URL test")
            return

        if not self.has_scratch_s3_bucket():
            logger.warning('No scratch S3 bucket, so skipping records directory URL test')
            return
        self.load_and_verify('delimited', 'bluelabs', sourcefn=self.s3_url_source)

    def test_load_from_gcs_records_directory(self):
        if not self.has_scratch_gcs_bucket():
            logger.warning('No scratch GCS bucket, so skipping records directory URL test')
            return
        self.load_and_verify('delimited', 'bluelabs', sourcefn=self.gcs_url_source)

    def records_filename(self, format_type, variant, hints={}, broken=False):
        basename = f"{self.resources_dir}/{self.resource_name(format_type, variant, hints)}.csv"
        if format_type == 'csv':
            default_format_compression = None
        else:
            default_format_compression = 'GZIP'
        if broken:
            basename = f"{basename}-broken"
        compression = hints.get('compression', default_format_compression)
        if compression == 'GZIP':
            return f"{basename}.gz"
        elif hints['compression'] is None:
            return basename
        else:
            raise ValueError(f"Teach me how to handle compression type {hints['compression']}")

    def gives_exact_load_count(self):
        return self.engine.name != 'redshift' and self.engine.name != 'vertica'

    def pull_schema_sql(self):
        with open(self.resources_dir +
                  f"/schema-{self.engine.name}.sql",
                  encoding='utf8') as fileobj:
            return fileobj.read().\
                replace("{schema_name}", self.schema_name).\
                replace("{table_name}", self.table_name)

    def local_source(self, filename, records_format, records_schema):
        return self.records.sources.local_file(filename=filename,
                                               records_format=records_format,
                                               records_schema=records_schema)

    @contextmanager
    def s3_url_source(self, filename, records_format, records_schema):
        base_dir = self.session.directory_url(self.session.creds.default_scratch_s3_url())

        with base_dir.temporary_directory() as temp_dir_loc:
            file_loc = temp_dir_loc.file_in_this_directory('foo.gz')
            with open(filename, mode='rb') as inp:
                file_loc.upload_fileobj(inp)
            yield self.records.sources.data_url(file_loc.url,
                                                records_format=records_format,
                                                records_schema=records_schema)

    @contextmanager
    def gcs_url_source(self, filename, records_format, records_schema):
        base_dir = self.session.directory_url(self.session.creds.default_scratch_gcs_url())

        with base_dir.temporary_directory() as temp_dir_loc:
            file_loc = temp_dir_loc.file_in_this_directory('foo.gz')
            with open(filename, mode='rb') as inp:
                file_loc.upload_fileobj(inp)
            yield self.records.sources.data_url(file_loc.url,
                                                records_format=records_format,
                                                records_schema=records_schema)

    def load(self, format_type, variant, hints={}, broken=False, sourcefn=None) -> None:
        if sourcefn is None:
            @contextmanager
            def sourcefn(filename, records_format, records_schema):
                yield self.local_source(filename, records_format, records_schema)

        records_format = RecordsFormat(format_type=format_type,
                                       variant=variant,
                                       hints=hints)
        #
        # CSV type inference is not smart enough to identify the
        # date/time columns as anything but strings yet.
        #
        # https://github.com/bluelabsio/records-mover/issues/80
        #
        # Once that's fixed, we can stop passing in a records schema
        # here when we have a header row for the names and let the
        # code figure it out itself, like this:
        #
        # records_schema = None
        # if hints.get('header-row', self.variant_has_header(variant)):
        #     # we can rely on the header to give us column names
        #     pass
        # else:
        #     records_schema_filename = f"{self.resources_dir}/_schema.json"
        #     with open(records_schema_filename, 'r') as f:
        #         records_schema = RecordsSchema.from_json(f.read())

        records_schema_filename = f"{self.resources_dir}/_schema.json"
        with open(records_schema_filename, 'r') as f:
            records_schema = RecordsSchema.from_json(f.read())

        targets = self.records.targets
        filename = self.records_filename(format_type, variant, hints, broken=broken)
        logger.info(f"Testing load from {filename}")

        target = targets.table(schema_name=self.schema_name,
                               table_name=self.table_name,
                               db_engine=self.engine)
        with sourcefn(filename=filename,
                      records_format=records_format,
                      records_schema=records_schema) as source:
            out = self.records.move(source, target)
        if not self.gives_exact_load_count():
            self.assertIsNone(out.move_count)
        else:
            self.assertIn(out.move_count, [None, 1])

    def verify_db_table(self, variant) -> None:
        validator = RecordsTableValidator(self.engine, file_variant=variant)
        validator.validate(schema_name=self.schema_name, table_name=self.table_name)
