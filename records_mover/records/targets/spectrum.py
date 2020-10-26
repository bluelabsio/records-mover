from .base import SupportsRecordsDirectory
from records_mover.db.quoting import quote_schema_and_table
from ...db import DBDriver
from ...url.resolver import UrlResolver
from ...url import BaseDirectoryUrl
from sqlalchemy.engine import Engine, Connection
from ..records_directory import RecordsDirectory
from ..records_format import ParquetRecordsFormat
from sqlalchemy.schema import CreateTable, MetaData, Table
from ..existing_table_handling import ExistingTableHandling
from typing import Optional, Callable, Union
import logging
import sqlalchemy


logger = logging.getLogger(__name__)


class SpectrumRecordsTarget(SupportsRecordsDirectory):
    def __init__(self,
                 schema_name: str,
                 table_name: str,
                 db_engine: Engine,
                 db_driver: Callable[[Union[Engine, Connection]], DBDriver],
                 url_resolver: UrlResolver,
                 spectrum_base_url: Optional[str],
                 spectrum_rdir_url: Optional[str],
                 existing_table_handling: ExistingTableHandling=
                 ExistingTableHandling.TRUNCATE_AND_OVERWRITE) -> None:
        self.db = db_engine
        self.driver = db_driver(db_engine)
        self.schema_name = schema_name
        self.table_name = table_name
        self.url_resolver = url_resolver
        self.existing_table_handling = existing_table_handling
        self.output_loc = self.calculate_output_url(spectrum_base_url=spectrum_base_url,
                                                    spectrum_rdir_url=spectrum_rdir_url)

        if not self.output_loc.scheme == 's3':
            raise ValueError("Records URL for Redshift Spectrum must be in S3")
        self.records_format = ParquetRecordsFormat()

    def calculate_output_url(self,
                             spectrum_base_url: Optional[str],
                             spectrum_rdir_url: Optional[str]) -> BaseDirectoryUrl:
        if spectrum_rdir_url is not None:
            return self.url_resolver.directory_url(spectrum_rdir_url)
        elif spectrum_base_url is not None:
            if not spectrum_base_url.endswith('/'):
                raise ValueError("Please provide a directory name - "
                                 f"URL should end with '/': {spectrum_base_url}")
            return\
                self.url_resolver.directory_url(spectrum_base_url).\
                directory_in_this_directory('data').\
                directory_in_this_directory(self.schema_name).\
                directory_in_this_directory(self.table_name)
        else:
            raise TypeError("Please specify either spectrum_base_url or spectrum_s3_rdir_url")

    def prep_bucket(self) -> None:
        # These are modes we may want to try to make work in the
        # future, but Redshift doesn't support them directly today so
        # we'd need to do some lower level things (e.g., truncating
        # the actual Parquet files)
        if self.existing_table_handling in [
                ExistingTableHandling.TRUNCATE_AND_OVERWRITE,
                ExistingTableHandling.DROP_AND_RECREATE,
                ExistingTableHandling.DELETE_AND_OVERWRITE
        ]:
            if self.existing_table_handling == ExistingTableHandling.DELETE_AND_OVERWRITE:
                logger.warning('Redshift Spectrum does not support transactional delete.')
            if self.existing_table_handling == ExistingTableHandling.DROP_AND_RECREATE:
                schema_and_table: str = quote_schema_and_table(self.db,
                                                               self.schema_name,
                                                               self.table_name)
                logger.info(f"Dropping external table {schema_and_table}...")
                with self.db.connect() as cursor:
                    # See below note about fix from Spectrify
                    cursor.execution_options(isolation_level='AUTOCOMMIT')
                    cursor.execute(f"DROP TABLE IF EXISTS {schema_and_table}")

            logger.info(f"Deleting files in {self.output_loc}...")
            self.output_loc.purge_directory()
        elif self.existing_table_handling == ExistingTableHandling.APPEND:
            raise NotImplementedError('APPEND mode not yet supported')
        else:
            raise NotImplementedError(f'Teach me how to handle {self.existing_table_handling}')

    def pre_load_hook(self) -> None:
        self.prep_bucket()

    def records_directory(self) -> RecordsDirectory:
        return RecordsDirectory(self.output_loc)

    def post_load_hook(self, num_rows_loaded: Optional[int]) -> None:
        if self.existing_table_handling != ExistingTableHandling.DROP_AND_RECREATE:
            return

        directory = self.records_directory()
        records_schema = directory.load_schema_json_obj()
        records_manifest = directory.get_manifest()
        assert records_manifest is not None  # just written before this was called
        assert records_schema is not None  # just written before this was called

        meta = MetaData()
        columns = [f.to_sqlalchemy_column(self.driver) for f in records_schema.fields]
        for column in columns:
            if isinstance(column.type, sqlalchemy.sql.sqltypes.Numeric) and column.type.asdecimal:
                # https://github.com/bluelabsio/records-mover/issues/85
                raise NotImplementedError("Teach me how to write a NUMERIC to Redshift Spectrum "
                                          f"(column name: {column})")
            if isinstance(column.type, sqlalchemy.sql.sqltypes.DateTime) and column.type.timezone:
                # https://github.com/bluelabsio/records-mover/issues/86
                raise NotImplementedError("Teach me how to write a datetimetz to Redshift Spectrum "
                                          f"({column})")

        table = Table(self.table_name, meta,
                      prefixes=["EXTERNAL"],
                      *columns,
                      schema=self.schema_name)
        schema_sql = str(CreateTable(table, bind=self.driver.db_engine))
        table_properties_clause = ''
        if num_rows_loaded is not None:
            table_properties_clause = f"\nTABLE PROPERTIES ('numRows'='{num_rows_loaded}')"
        else:
            # If the move from the source doesn't give us a row count
            # for use as statistics by the Redshift query optimizer,
            # we could potentially gather it manually:
            #
            # https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_TABLE_external-table.html
            #
            # https://github.com/bluelabsio/records-mover/issues/87
            pass
        storage_clause = "STORED AS PARQUET\n"
        location_clause = f"LOCATION '{self.output_loc.url}_manifest'\n"
        schema_sql = schema_sql + storage_clause + location_clause + table_properties_clause

        logger.info(schema_sql)
        with self.driver.db_engine.connect() as cursor:
            # without autocommit, we get "CREATE EXTERNAL TABLE cannot
            # run inside a transaction block"
            #
            # Fix from Spectrify:
            #
            # https://github.com/hellonarrativ/spectrify/issues/12
            # https://github.com/hellonarrativ/spectrify/pull/13/files#diff-d604d159eb266aacefeebac327bacd26R57

            cursor.execution_options(isolation_level='AUTOCOMMIT')
            cursor.execute(schema_sql)
