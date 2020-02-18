# https://github.com/googleapis/google-cloud-python/blob/1e19f9b65a8610c22a69789c89c8b7fd442505ff/bigquery/google/cloud/bigquery/dataset.py
import google.cloud.bigquery.table


class DatasetReference:
    def table(self, table_id: str) -> google.cloud.bigquery.table.TableReference:
        ...
