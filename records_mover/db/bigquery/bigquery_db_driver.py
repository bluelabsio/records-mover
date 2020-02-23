from ..driver import DBDriver
import logging
from ...records import RecordsSchema
from ...records.unload_plan import RecordsUnloadPlan
from ...records.load_plan import RecordsLoadPlan
from ...records.records_format import BaseRecordsFormat, ParquetRecordsFormat
from ...records.types import RecordsFormatType
from ...records.records_directory import RecordsDirectory
from ...utils.limits import INT64_MAX, INT64_MIN, FLOAT64_SIGNIFICAND_BITS, num_digits
import re
from typing import Union, Optional, List, Tuple, IO
from ...url.resolver import UrlResolver
import sqlalchemy
from .loader import BigQueryLoader

logger = logging.getLogger(__name__)


class BigQueryDBDriver(DBDriver):
    def __init__(self,
                 db: Union[sqlalchemy.engine.Connection, sqlalchemy.engine.Engine],
                 url_resolver: UrlResolver,
                 **kwargs) -> None:
        super().__init__(db)
        self._bigquery_loader = BigQueryLoader(db=self.db, url_resolver=url_resolver)

    def load(self,
             schema: str,
             table: str,
             load_plan: RecordsLoadPlan,
             directory: RecordsDirectory) -> int:
        """Loads the data from the RecordsDirectory instance named
        'directory'.  Guarantees a manifest file named 'manifest' is
        written to the target directory pointing to the target
        records.

        Returns number of rows loaded (if database provides that
        info).

        """
        return self._bigquery_loader.load(schema=schema,
                                          table=table,
                                          load_plan=load_plan,
                                          directory=directory)

    def load_from_fileobj(self, schema: str, table: str,
                          load_plan: RecordsLoadPlan, fileobj: IO[bytes]) -> Optional[int]:
        return self._bigquery_loader.load_from_fileobj(schema=schema,
                                                       table=table,
                                                       load_plan=load_plan,
                                                       fileobj=fileobj)

    def can_load_from_fileobjs(self) -> bool:
        return True

    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        return self._bigquery_loader.can_load_this_format(source_records_format)

    def known_supported_records_formats_for_load(self) -> List[BaseRecordsFormat]:
        return self._bigquery_loader.known_supported_records_formats_for_load()

    def unload(self,
               schema: str,
               table: str,
               unload_plan: RecordsUnloadPlan,
               directory: RecordsDirectory) -> int:
        """Writes table specified to the RecordsDirectory instance named 'directory'
        per the UnloadPlan named 'unload_plan'.  Guarantees a manifest
        file named 'manifest' is written to the target directory pointing
        to the target records.  After ensuring other records-related
        metadata is provided, you can call directory.finalize_manifest()
        to move this to the final location of _manifest, signaling to
        readers that the Records directory is finalized.

        Returns number of rows unloaded.
        """
        raise NotImplementedError(f"unload not implemented for this database type")

    def can_unload_this_format(self, target_records_format: BaseRecordsFormat) -> bool:
        return False

    def known_supported_records_formats_for_unload(self) -> List[BaseRecordsFormat]:
        return []

    def best_records_format_variant(self, records_format_type: RecordsFormatType) ->\
            Optional[str]:
        if records_format_type == 'delimited':
            return 'bigquery'
        else:
            return None

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
            return records_schema.convert_datetimes_to_datetimetz()
        else:
            return records_schema
