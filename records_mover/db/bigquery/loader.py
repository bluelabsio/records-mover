from contextlib import contextmanager
from typing import Union, List, IO, Tuple, Optional, Iterator
from ...url import BaseDirectoryUrl
from ...records.delimited import complain_on_unhandled_hints
import pprint
import sqlalchemy
from ...records.load_plan import RecordsLoadPlan
from ...records.records_format import (
    BaseRecordsFormat, DelimitedRecordsFormat, ParquetRecordsFormat, AvroRecordsFormat
)
from ...records.records_directory import RecordsDirectory
from ...records.processing_instructions import ProcessingInstructions
from ...url.resolver import UrlResolver
from google.cloud.bigquery.dbapi.connection import Connection
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.job import LoadJobConfig
from google.cloud.exceptions import NotFound
from .load_job_config_options import load_job_config
import logging
from ..loader import LoaderFromFileobj
from ..errors import NoTemporaryBucketConfiguration

logger = logging.getLogger(__name__)


class BigQueryLoader(LoaderFromFileobj):
    def __init__(self,
                 db: Union[sqlalchemy.engine.Connection, sqlalchemy.engine.Engine],
                 url_resolver: UrlResolver,
                 gcs_temp_base_loc: Optional[BaseDirectoryUrl])\
            -> None:
        self.db = db
        self.url_resolver = url_resolver
        self.gcs_temp_base_loc = gcs_temp_base_loc

    def best_scheme_to_load_from(self) -> str:
        return 'gs'

    @contextmanager
    def temporary_gcs_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        if self.gcs_temp_base_loc is None:
            raise NoTemporaryBucketConfiguration('Please provide a scratch GCS URL in your config '
                                                 '(e.g., set SCRATCH_GCS_URL to a gs:// URL)')
        else:
            with self.gcs_temp_base_loc.temporary_directory() as temp_loc:
                yield temp_loc

    def _parse_bigquery_schema_name(self, schema: str) -> Tuple[Optional[str], str]:
        # https://github.com/mxmzdlv/pybigquery/blob/master/pybigquery/sqlalchemy_bigquery.py#L320
        dataset = None
        project = None

        schema_split = schema.split('.')
        if len(schema_split) == 1:
            dataset, = schema_split
        elif len(schema_split) == 2:
            project, dataset = schema_split
        else:
            raise ValueError(f"Could not understand schema name {schema}")

        return (project, dataset)

    def _load_job_config(self, load_plan: RecordsLoadPlan) -> LoadJobConfig:
        target_records_format = load_plan.records_format
        processing_instructions = load_plan.processing_instructions
        unhandled_hints = set()
        if isinstance(target_records_format, DelimitedRecordsFormat):
            unhandled_hints = set(target_records_format.hints.keys())
        job_config = load_job_config(unhandled_hints, load_plan)
        if isinstance(target_records_format, DelimitedRecordsFormat):
            complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                        unhandled_hints, target_records_format.hints)
        return job_config

    def load_from_fileobj(self, schema: str, table: str,
                          load_plan: RecordsLoadPlan, fileobj: IO[bytes]) -> int:
        logger.info("Loading from fileobj into BigQuery")
        # https://googleapis.github.io/google-cloud-python/latest/bigquery/usage/tables.html#creating-a-table
        connection: Connection =\
            self.db.engine.raw_connection().connection
        # https://google-cloud.readthedocs.io/en/latest/bigquery/generated/google.cloud.bigquery.client.Client.html
        client: Client = connection._client
        project_id, dataset_id = self._parse_bigquery_schema_name(schema)
        # https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.job.LoadJobConfig.html
        job_config = self._load_job_config(load_plan)

        logger.info(f"Using BigQuery load options: {job_config.to_api_repr()}")
        # https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.load_table_from_file
        job = client.load_table_from_file(fileobj,
                                          f"{schema}.{table}",
                                          job_config=job_config)

        try:
            job.result()  # Waits for table load to complete.
        except Exception:
            logger.error(f"BigQuery load errors:\n\n{pprint.pformat(job.errors)}\n")
            raise

        logger.info(f"Loaded {job.output_rows} rows into {dataset_id}:{table}")
        assert job.output_rows is not None  # should be populated after job result is obtained
        return job.output_rows

    def load(self,
             schema: str,
             table: str,
             load_plan: RecordsLoadPlan,
             directory: RecordsDirectory) -> Optional[int]:

        if directory.scheme != 'gs':
            if self.has_temporary_loadable_directory_loc():
                with self.temporary_gcs_directory_loc() as temp_gcs_loc:
                    gcs_directory = directory.copy_to(temp_gcs_loc)
                    return self.load(schema=schema,
                                     table=table,
                                     load_plan=load_plan,
                                     directory=gcs_directory)
            else:
                return self.load_from_records_directory_via_fileobj(schema=schema,
                                                                    table=table,
                                                                    load_plan=load_plan,
                                                                    directory=directory)

        logger.info("Loading from records directory into BigQuery")
        # https://googleapis.github.io/google-cloud-python/latest/bigquery/usage/tables.html#creating-a-table
        connection: Connection = self.db.engine.raw_connection().connection
        # https://google-cloud.readthedocs.io/en/latest/bigquery/generated/google.cloud.bigquery.client.Client.html
        client: Client = connection._client
        project_id, dataset_id = self._parse_bigquery_schema_name(schema)
        job_config = self._load_job_config(load_plan)

        try:
            table_obj = client.get_table(f"{schema}.{table}")
        except NotFound:
            logger.error("BigQuery table %s.%s not found", schema, table)
            raise

        all_urls = directory.manifest_entry_urls()

        job = client.load_table_from_uri(all_urls,
                                         f"{schema}.{table}",
                                         # Must match the destination dataset location.
                                         location=table_obj.location,
                                         job_config=job_config)
        try:
            job.result()  # Waits for table load to complete.
        except Exception:
            logger.error(f"BigQuery load errors:\n\n{pprint.pformat(job.errors)}\n")
            raise

        logger.info(f"Loaded {job.output_rows} rows into {dataset_id}:{table}")
        assert job.output_rows is not None  # should be populated after job result is obtained
        return job.output_rows

    def temporary_loadable_directory_scheme(self) -> str:
        return 'gs'

    @contextmanager
    def temporary_loadable_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        with self.temporary_gcs_directory_loc() as temp_loc:
            yield temp_loc

    def has_temporary_loadable_directory_loc(self) -> bool:
        return self.gcs_temp_base_loc is not None

    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        try:
            processing_instructions = ProcessingInstructions()
            load_plan = RecordsLoadPlan(records_format=source_records_format,
                                        processing_instructions=processing_instructions)
            if isinstance(load_plan.records_format, ParquetRecordsFormat):
                return True
            if isinstance(load_plan.records_format, AvroRecordsFormat):
                return True
            if not isinstance(load_plan.records_format, DelimitedRecordsFormat):
                return False
            unhandled_hints = set(load_plan.records_format.hints.keys())
            load_job_config(unhandled_hints, load_plan)
            complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                        unhandled_hints, load_plan.records_format.hints)
            return True
        except NotImplementedError:
            return False

    def known_supported_records_formats_for_load(self) -> List[BaseRecordsFormat]:
        return [
            DelimitedRecordsFormat(variant='bigquery'),
            ParquetRecordsFormat(),
            AvroRecordsFormat()
        ]
