# https://github.com/googleapis/google-cloud-python/blob/ff05ac3a8fa58a696584bada6ac89eb0a81e9856/bigquery/google/cloud/bigquery/client.py
from typing import Optional, IO, Union, Sequence
from google.cloud.bigquery.dataset import DatasetReference
import google.cloud.bigquery.job
import google.cloud.bigquery.table

_DEFAULT_NUM_RETRIES = 6


class Client:
    def dataset(self, dataset_id: str, project: Optional[str]) -> DatasetReference:
        ...

    def load_table_from_file(self,
                             file_obj: IO[bytes],
                             destination: Union[google.cloud.bigquery.table.TableReference, str],
                             rewind: bool = False,
                             size: Optional[int] = None,
                             num_retries: int = _DEFAULT_NUM_RETRIES,
                             job_id: Optional[str] = None,
                             job_id_prefix: Optional[str] = None,
                             location: Optional[str] = None,
                             project: Optional[str] = None,
                             job_config: Optional[google.cloud.bigquery.job.LoadJobConfig] =
                             None) -> google.cloud.bigquery.job.LoadJob:
        ...

    def load_table_from_uri(self,
                            source_uris: Union[str,
                                               Sequence[str]],
                            destination: Union[google.cloud.bigquery.table.Table,
                                               google.cloud.bigquery.table.TableReference,
                                               str],
                            job_id: Optional[str] = None,
                            job_id_prefix: Optional[str] = None,
                            location: Optional[str] = None,
                            project: Optional[str] = None,
                            job_config: Optional[google.cloud.bigquery.job.LoadJobConfig] = None,
                            retry: Optional[google.api_core.retry.Retry] = None,
                            timeout: Optional[float] = None) -> google.cloud.bigquery.job.LoadJob:
        ...

    def extract_table(self,
                      source: Union[google.cloud.bigquery.table.Table,
                                    google.cloud.bigquery.table.TableReference,
                                    str],
                      destination_uris: Union[str, Sequence[str]],
                      job_id: Optional[str] = None,
                      job_id_prefix: Optional[str] = None,
                      location: Optional[str] = None,
                      project: Optional[str] = None,
                      job_config: Optional[google.cloud.bigquery.job.ExtractJobConfig] = None,
                      retry: Optional[google.api_core.retry.Retry] = None,
                      timeout: Optional[float] = None,
                      source_type: Optional[str] = None) -> google.cloud.bigquery.job.ExtractJob:
        ...

    def get_table(self,
                  table: Union[google.cloud.bigquery.table.Table,
                               google.cloud.bigquery.table.TableReference,
                               str],
                  retry: Optional[google.api_core.retry.Retry] = None,
                  timeout: Optional[float] = None) -> google.cloud.bigquery.table.Table:
        ...

