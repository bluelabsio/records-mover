import logging
import tempfile
import pathlib
from ..directory_validator import RecordsDirectoryValidator
from ..records_database_fixture import RecordsDatabaseFixture
from .base_records_test import BaseRecordsIntegrationTest
from records_mover.records import DelimitedRecordsFormat, move

logger = logging.getLogger(__name__)


class RecordsUnloadIntegrationTest(BaseRecordsIntegrationTest):
    def test_unload_csv_format(self):
        self.unload_and_verify('delimited', 'csv')

    def test_unload_csv_format_without_header_row(self):
        self.unload_and_verify('delimited', 'csv', {'header-row': False})

    def test_unload_bluelabs_format(self):
        self.unload_and_verify('delimited', 'bluelabs')

    def test_unload_bluelabs_format_with_header_row(self):
        self.unload_and_verify('delimited', 'bluelabs', {'header-row': True})

    def test_unload_vertica_format(self):
        self.unload_and_verify('delimited', 'vertica')

    def test_unload_vertica_format_with_header_row(self):
        self.unload_and_verify('delimited', 'vertica', {'header-row': True})

    def test_unload_bigquery_format(self):
        self.unload_and_verify('delimited', 'bigquery')

    def test_unload_bigquery_format_without_header_row(self):
        self.unload_and_verify('delimited', 'bigquery', {'header-row': False})

    def requires_pandas(self, format_type, variant, hints):
        # Return true if the database requires pandas to be able to
        # export the given format and variant
        if self.engine.name == 'redshift':
            if format_type == 'delimited':
                return (variant in ['csv', 'bigquery', 'vertica'] or
                        hints.get('header-row', self.variant_has_header(variant)))
        logger.info(f"Engine {self.engine.name} is OK without pandas for {format_type}/{variant}")
        return False

    def unload_and_verify(self, format_type, variant, hints={}):
        if (not self.has_pandas()) and self.requires_pandas(format_type, variant, hints):
            logger.warning("Skipping test as we don't have Pandas to export with.")
            return
        fixture = RecordsDatabaseFixture(self.engine,
                                         schema_name=self.schema_name,
                                         table_name=self.table_name)
        fixture.bring_up()
        with tempfile.TemporaryDirectory(prefix='test_records_unload') as tempdir:
            self.unload(variant, tempdir, hints=hints)
            self.verify_records_directory(format_type, variant, tempdir, hints=hints)

    def unload(self, variant, directory, hints={}):
        records_format = DelimitedRecordsFormat(variant=variant,
                                                hints=hints)

        directory_url = pathlib.Path(directory).resolve().as_uri() + '/'
        targets = self.records.targets
        sources = self.records.sources
        source = sources.table(schema_name=self.schema_name,
                               table_name=self.table_name,
                               db_engine=self.engine)
        target = targets.directory_from_url(output_url=directory_url,
                                            records_format=records_format)
        out = move(source, target)
        self.assertTrue(out.move_count in [1, None])

    def verify_records_directory(self, format_type, variant, tempdir, hints={}):
        validator = RecordsDirectoryValidator(tempdir,
                                              self.resource_name(format_type, variant,
                                                                 hints))
        validator.validate()
