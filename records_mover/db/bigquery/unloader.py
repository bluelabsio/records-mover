import sqlalchemy
from contextlib import contextmanager
from typing import List, Iterator, Optional, Union, Tuple
import logging
from google.cloud.bigquery.dbapi.connection import Connection
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.job import ExtractJobConfig
from records_mover.db.unloader import Unloader
from records_mover.records.records_format import BaseRecordsFormat, AvroRecordsFormat
from records_mover.url.base import BaseDirectoryUrl
from records_mover.url.resolver import UrlResolver
from records_mover.records.unload_plan import RecordsUnloadPlan
from records_mover.records.records_directory import RecordsDirectory
from records_mover.db.errors import NoTemporaryBucketConfiguration

logger = logging.getLogger(__name__)


class BigQueryUnloader(Unloader):
    def __init__(self,
                 db: Union[sqlalchemy.engine.Connection, sqlalchemy.engine.Engine],
                 url_resolver: UrlResolver,
                 gcs_temp_base_loc: Optional[BaseDirectoryUrl])\
            -> None:
        self.db = db
        self.url_resolver = url_resolver
        self.gcs_temp_base_loc = gcs_temp_base_loc
        super().__init__(db=db)

    def can_unload_format(self, target_records_format: BaseRecordsFormat) -> bool:
        if isinstance(target_records_format, AvroRecordsFormat):
            return True
        return False

    def can_unload_to_scheme(self, scheme: str) -> bool:
        if scheme == 'gs':
            return True
        # Otherwise we'll need a temporary bucket configured for
        # BigQuery to unload into
        return self.gcs_temp_base_loc is not None

    def known_supported_records_formats_for_unload(self) -> List[BaseRecordsFormat]:
        return [AvroRecordsFormat()]

    @contextmanager
    def temporary_unloadable_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
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

    def _extract_job_config(self, unload_plan: RecordsUnloadPlan) -> ExtractJobConfig:
        config = ExtractJobConfig()
        if isinstance(unload_plan.records_format, AvroRecordsFormat):
            config.destination_format = 'AVRO'
            # https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#logical_types
            config.use_avro_logical_types = True
        else:
            raise NotImplementedError(f'Please add support for {unload_plan.records_format}')
        return config

    def unload(self,
               schema: str,
               table: str,
               unload_plan: RecordsUnloadPlan,
               directory: RecordsDirectory) -> Optional[int]:
        if directory.scheme != 'gs':
            with self.temporary_unloadable_directory_loc() as temp_gcs_loc:
                temp_directory = RecordsDirectory(temp_gcs_loc)
                out = self.unload(schema=schema,
                                  table=table,
                                  unload_plan=unload_plan,
                                  directory=temp_directory)
                temp_directory.copy_to(directory.loc)
                return out
        logger.info("Loading from records directory into BigQuery")
        # https://googleapis.github.io/google-cloud-python/latest/bigquery/usage/tables.html#creating-a-table
        connection: Connection =\
            self.db.engine.raw_connection().connection
        # https://google-cloud.readthedocs.io/en/latest/bigquery/generated/google.cloud.bigquery.client.Client.html
        client: Client = connection._client
        project_id, dataset_id = self._parse_bigquery_schema_name(schema)
        job_config = self._extract_job_config(unload_plan)

        records_format = unload_plan.records_format
        filename = records_format.generate_filename('output')
        destination_uri = directory.loc.file_in_this_directory(filename)
        job = client.extract_table(f"{schema}.{table}",
                                   destination_uri.url,
                                   # Must match the destination dataset location.
                                   job_config=job_config)
        job.result()  # Waits for table load to complete.
        logger.info(f"Unloaded from {dataset_id}:{table} into {filename}")
        directory.save_preliminary_manifest()
        return None
