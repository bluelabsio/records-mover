import logging
import tempfile
import pathlib
from ..directory_validator import RecordsDirectoryValidator
from ..records_database_fixture import RecordsDatabaseFixture
from .base_records_test import BaseRecordsIntegrationTest

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

    def unload_and_verify(self, format_type, variant, hints={}):
        fixture = RecordsDatabaseFixture(self.engine,
                                         schema_name=self.schema_name,
                                         table_name=self.table_name)
        fixture.bring_up()
        with tempfile.TemporaryDirectory(prefix='test_records_unload') as tempdir:
            self.unload(variant, tempdir, hints=hints)
            self.verify_records_directory(format_type, variant, tempdir, hints=hints)

    def this_vertica_supports_s3(self):
        out = self.engine.execute("SELECT lib_name "
                                  "FROM user_libraries "
                                  "WHERE lib_name = 'awslib'")
        return len(list(out.fetchall())) == 1

    def unload(self, variant, directory, hints={}):
        records_format = self.records.RecordsFormat(format_type='delimited',
                                                    variant=variant,

                                                    hints=hints)

        directory_url = pathlib.Path(directory).resolve().as_uri() + '/'
        targets = self.records.targets
        sources = self.records.sources
        move = self.records.move
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
