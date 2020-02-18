from typing import Union, List, IO, Tuple, Optional
from ...records.hints import complain_on_unhandled_hints
import pprint
import sqlalchemy
from ...records.load_plan import RecordsLoadPlan
from ...records.records_format import (
    BaseRecordsFormat, DelimitedRecordsFormat, ParquetRecordsFormat
)
from ...records.records_directory import RecordsDirectory
from ...records.processing_instructions import ProcessingInstructions
from ...url.resolver import UrlResolver
from google.cloud.bigquery.dbapi.connection import Connection
from google.cloud.bigquery.client import Client
from .load_job_config_options import load_job_config
import logging

logger = logging.getLogger(__name__)


class BigQueryLoader:
    def __init__(self,
                 db: Union[sqlalchemy.engine.Connection, sqlalchemy.engine.Engine],
                 url_resolver: UrlResolver) -> None:
        self.db = db
        self.url_resolver = url_resolver

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

    def load_from_fileobj(self, schema: str, table: str,
                          load_plan: RecordsLoadPlan, fileobj: IO[bytes]) -> int:
        # https://googleapis.github.io/google-cloud-python/latest/bigquery/usage/tables.html#creating-a-table
        connection: Connection =\
            self.db.engine.raw_connection().connection
        # https://google-cloud.readthedocs.io/en/latest/bigquery/generated/google.cloud.bigquery.client.Client.html
        client: Client = connection._client
        project_id, dataset_id = self._parse_bigquery_schema_name(schema)
        dataset_ref = client.dataset(dataset_id, project_id)
        table_ref = dataset_ref.table(table)
        # https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.job.LoadJobConfig.html

        target_records_format = load_plan.records_format
        processing_instructions = load_plan.processing_instructions
        unhandled_hints = set()
        if isinstance(target_records_format, DelimitedRecordsFormat):
            unhandled_hints = set(target_records_format.hints.keys())
        job_config = load_job_config(unhandled_hints, load_plan)
        if isinstance(target_records_format, DelimitedRecordsFormat):
            complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                        unhandled_hints, target_records_format.hints)
        logger.info(f"Using BigQuery load options: {job_config.to_api_repr()}")
        job = client.load_table_from_file(fileobj,
                                          table_ref,
                                          # Must match the destination dataset location.
                                          location="US",
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
             directory: RecordsDirectory) -> int:
        all_urls = directory.manifest_entry_urls()

        total_rows = 0

        for url in all_urls:
            loc = self.url_resolver.file_url(url)
            with loc.open() as f:
                # We set WriteDisposition.WRITE_APPEND in
                # load_options_config.py and let the records Prep
                # class decide what to do about the existing table, so
                # it's safe to call this multiple times and append until
                # done:
                logger.info(f"Loading {url} into {schema}.{table}")
                total_rows += self.load_from_fileobj(schema, table, load_plan, f)
        return total_rows

    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        try:
            processing_instructions = ProcessingInstructions()
            load_plan = RecordsLoadPlan(records_format=source_records_format,
                                        processing_instructions=processing_instructions)
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
        return [DelimitedRecordsFormat(variant='bigquery'), ParquetRecordsFormat()]
