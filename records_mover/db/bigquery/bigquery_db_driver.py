from ..driver import DBDriver
import logging
from ...records import RecordsSchema
from ...records.records_format import BaseRecordsFormat, ParquetRecordsFormat, AvroRecordsFormat
from ...utils.limits import INT64_MAX, INT64_MIN, FLOAT64_SIGNIFICAND_BITS, num_digits
import re
from typing import Union, Optional, Tuple
from ...url.resolver import UrlResolver
import sqlalchemy
from .loader import BigQueryLoader
from .unloader import BigQueryUnloader
from ..loader import LoaderFromFileobj, LoaderFromRecordsDirectory
from ..unloader import Unloader
from ...url.base import BaseDirectoryUrl


logger = logging.getLogger(__name__)


class BigQueryDBDriver(DBDriver):
    def __init__(self,
                 db: Union[sqlalchemy.engine.Connection, sqlalchemy.engine.Engine],
                 url_resolver: UrlResolver,
                 gcs_temp_base_loc: Optional[BaseDirectoryUrl] = None,
                 **kwargs: object) -> None:
        super().__init__(db)
        self._bigquery_loader =\
            BigQueryLoader(db=self.db,
                           url_resolver=url_resolver,
                           gcs_temp_base_loc=gcs_temp_base_loc)
        self._bigquery_unloader =\
            BigQueryUnloader(db=self.db,
                             url_resolver=url_resolver,
                             gcs_temp_base_loc=gcs_temp_base_loc)

    def loader(self) -> Optional[LoaderFromRecordsDirectory]:
        return self._bigquery_loader

    def loader_from_fileobj(self) -> LoaderFromFileobj:
        return self._bigquery_loader

    def unloader(self) -> Unloader:
        return self._bigquery_unloader

    def type_for_date_plus_time(self, has_tz: bool=False) -> sqlalchemy.sql.sqltypes.DateTime:
        # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
        if has_tz:
            # pybigquery doesn't set the timezone flag :(
            return sqlalchemy.sql.sqltypes.TIMESTAMP()
        else:
            return sqlalchemy.sql.sqltypes.DATETIME()

    def make_column_name_valid(self, colname: str) -> str:
        # https://cloud.google.com/bigquery/docs/schemas#column_names

        # replace anything which isn't a letter or number with '_',
        # which is the only valid symbol
        return re.sub(r'[^A-Za-z0-9]', '_', colname)

    def integer_limits(self,
                       type_: sqlalchemy.types.Integer) ->\
            Optional[Tuple[int, int]]:
        # Only one int type for BigQuery, represented as sqlalchemy.types.Integer...
        return (INT64_MIN, INT64_MAX)

    def fp_constraints(self,
                       type_: sqlalchemy.types.Float) ->\
            Optional[Tuple[int, int]]:
        # Only one float type for BigQuery, represented as sqlalchemy.types.Float...
        return (64, FLOAT64_SIGNIFICAND_BITS)

    def fixed_point_constraints(self,
                                type_: sqlalchemy.types.Numeric) ->\
            Optional[Tuple[int, int]]:
        if isinstance(type_, sqlalchemy.sql.sqltypes.DECIMAL):
            return (38, 9)
        else:
            logger.warning(f"Don't know how to handle unexpected BigQuery type {type(type_)}")
            return None

    def type_for_fixed_point(self,
                             precision: int,
                             scale: int) -> sqlalchemy.sql.sqltypes.Numeric:
        # BigQuery NUMERIC() type takes no arguments and supports 38
        # digits of precision, of which 9 digits are scale.
        if precision > 38 or scale > 9:
            logger.warning('Using BigQuery FLOAT64 type to represent '
                           f'NUMERIC({precision},{scale}))')
            return sqlalchemy.types.Float()
        return sqlalchemy.sql.sqltypes.Numeric()

    def type_for_integer(self,
                         min_value: Optional[int],
                         max_value: Optional[int]) -> sqlalchemy.types.TypeEngine:
        """Find correct integral column type to fit the given min and max integer values"""

        if min_value is not None and max_value is not None:
            if min_value >= INT64_MIN and max_value <= INT64_MAX:
                # BigQuery has only 64-bit integers
                return sqlalchemy.sql.sqltypes.Integer()
            else:
                num_digits_min = num_digits(min_value)
                num_digits_max = num_digits(max_value)
                digit_count = max(num_digits_min, num_digits_max)
                return self.type_for_fixed_point(precision=digit_count,
                                                 scale=0)
        return super().type_for_integer(min_value, max_value)

    def tweak_records_schema_for_load(self,
                                      records_schema: RecordsSchema,
                                      records_format: BaseRecordsFormat) -> RecordsSchema:
        if isinstance(records_format, ParquetRecordsFormat):
            # According to Google, "DATETIME is not supported for
            # uploading from Parquet" -
            # https://github.com/googleapis/google-cloud-python/issues/9996#issuecomment-572273407
            #
            # So we need to make sure we don't create any DATETIME
            # columns if we're loading from a Parquet file.
            #
            # https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet
            return records_schema.cast_field_types({'datetime': 'datetimetz'})
        else:
            return records_schema

    def tweak_records_schema_after_unload(self,
                                          records_schema: RecordsSchema,
                                          records_format: BaseRecordsFormat) -> RecordsSchema:
        if isinstance(records_format, AvroRecordsFormat):
            # https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#logical_types
            #
            # "Note: There is no logical type that directly
            # corresponds to DATETIME, and BigQuery currently doesn't
            # support any direct conversion from an Avro type into a
            # DATETIME field."
            #
            # BigQuery exports this as an Avro string type
            return records_schema.cast_field_types({'datetime': 'string'})
        else:
            return records_schema
