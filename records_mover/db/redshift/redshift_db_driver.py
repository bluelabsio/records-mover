from ..driver import DBDriver
from ...records.records_directory import RecordsDirectory
import sqlalchemy
from sqlalchemy.schema import Table
import logging
from ...records.load_plan import RecordsLoadPlan
from ...records.records_format import BaseRecordsFormat
from ...records.unload_plan import RecordsUnloadPlan
from ...utils.limits import (INT16_MIN, INT16_MAX,
                             INT32_MIN, INT32_MAX,
                             INT64_MIN, INT64_MAX,
                             FLOAT32_SIGNIFICAND_BITS,
                             FLOAT64_SIGNIFICAND_BITS,
                             num_digits)
from .sql import schema_sql_from_admin_views
import timeout_decorator
from contextlib import contextmanager
from typing import Iterator, Optional, Union, Dict, List, Tuple
from ...url.base import BaseDirectoryUrl
from records_mover.db.quoting import quote_group_name, quote_schema_and_table
from .unloader import RedshiftUnloader
from .loader import RedshiftLoader
from ..errors import NoTemporaryBucketConfiguration

logger = logging.getLogger(__name__)


class RedshiftDBDriver(DBDriver):
    def __init__(self,
                 db: Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection],
                 s3_temp_base_loc: Optional[BaseDirectoryUrl]=None,
                 **kwargs) -> None:
        super().__init__(db)
        self.s3_temp_base_loc = s3_temp_base_loc
        self._redshift_loader =\
            RedshiftLoader(db=db,
                           meta=self.meta,
                           temporary_s3_directory_loc=self.temporary_s3_directory_loc)
        self._redshift_unloader =\
            RedshiftUnloader(db=db,
                             table=self.table,
                             temporary_s3_directory_loc=self.temporary_s3_directory_loc)

    def best_scheme_to_load_from(self) -> str:
        return 's3'

    def schema_sql(self, schema: str, table: str) -> str:
        out = schema_sql_from_admin_views(schema, table, self.db)
        if out is None:
            return super().schema_sql(schema=schema, table=table)
        else:
            return out

    @contextmanager
    def temporary_s3_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        if self.s3_temp_base_loc is None:
            raise NoTemporaryBucketConfiguration('Please provide a scratch S3 URL in your config')
        else:
            with self.s3_temp_base_loc.temporary_directory() as temp_loc:
                yield temp_loc

    @contextmanager
    def temporary_loadable_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        with self.temporary_s3_directory_loc() as temp_loc:
            yield temp_loc

    # if this timeout goes off (at least for Redshift), it's probably
    # because memory is filling because sqlalchemy's cache of all
    # tables and columns filled up memory in the job.
    @timeout_decorator.timeout(80)
    def table(self, schema: str, table: str) -> Table:
        with self.db.engine.connect() as conn:
            with conn.begin():
                # The code in the Redshift SQLAlchemy driver relies on 'SET
                # LOCAL search_path' in order to do reflection and pull back
                # column information on tables.
                #
                # Unfortunately, 'SET LOCAL' only works when a transaction is
                # in process, and it looks like there's often not here, and an
                # exception will be thrown unless the table you're working
                # with is in your default schema.
                #
                # There was some hang-wringing when they added it originally:
                #  https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/commit/
                #      94723ec6437c5e5197fcf785845499e81640b167
                return Table(table, self.meta, schema=schema, autoload=True, autoload_with=conn)

    def load(self,
             schema: str,
             table: str,
             load_plan: RecordsLoadPlan,
             directory: RecordsDirectory) -> Optional[int]:
        return self._redshift_loader.load(schema=schema,
                                          table=table,
                                          load_plan=load_plan,
                                          directory=directory)

    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        return self._redshift_loader.can_load_this_format(source_records_format)

    def known_supported_records_formats_for_load(self) -> List[BaseRecordsFormat]:
        return self._redshift_loader.known_supported_records_formats_for_load()

    def unload(self,
               schema: str,
               table: str,
               unload_plan: RecordsUnloadPlan,
               directory: RecordsDirectory) -> int:
        return self._redshift_unloader.unload(schema=schema,
                                              table=table,
                                              unload_plan=unload_plan,
                                              directory=directory)

    def can_unload_this_format(self, target_records_format: BaseRecordsFormat) -> bool:
        return self._redshift_unloader.can_unload_this_format(target_records_format)

    def known_supported_records_formats_for_unload(self) -> List[BaseRecordsFormat]:
        return self._redshift_unloader.known_supported_records_formats_for_unload()

    def set_grant_permissions_for_groups(self, schema_name: str, table: str,
                                         groups: Dict[str, List[str]],
                                         db: Union[sqlalchemy.engine.Engine,
                                                   sqlalchemy.engine.Connection]) -> None:
        schema_and_table = quote_schema_and_table(self.db.engine, schema_name, table)
        for perm_type in groups:
            groups_list = groups[perm_type]
            for group in groups_list:
                group_name: str = quote_group_name(self.db.engine, group)
                if not perm_type.isalpha():
                    raise TypeError("Please make sure your permission types"
                                    " are an acceptable value.")
                perms_sql = f'GRANT {perm_type} ON TABLE {schema_and_table} TO GROUP {group_name}'
                db.execute(perms_sql)
        return None

    def supports_time_type(self):
        return False

    def integer_limits(self,
                       type_: sqlalchemy.types.Integer) ->\
            Optional[Tuple[int, int]]:
        if isinstance(type_, sqlalchemy.sql.sqltypes.SMALLINT):
            return (INT16_MIN, INT16_MAX)
        elif isinstance(type_, sqlalchemy.sql.sqltypes.INTEGER):
            return (INT32_MIN, INT32_MAX)
        elif isinstance(type_, sqlalchemy.sql.sqltypes.BIGINT):
            return (INT64_MIN, INT64_MAX)
        return super().integer_limits(type_)

    def fp_constraints(self,
                       type_: sqlalchemy.types.Float) ->\
            Optional[Tuple[int, int]]:
        if isinstance(type_, sqlalchemy.dialects.postgresql.base.DOUBLE_PRECISION):
            return (64, FLOAT64_SIGNIFICAND_BITS)
        elif isinstance(type_, sqlalchemy.sql.sqltypes.REAL):
            return (32, FLOAT32_SIGNIFICAND_BITS)
        return super().fp_constraints(type_)

    def type_for_integer(self,
                         min_value: Optional[int],
                         max_value: Optional[int]) -> sqlalchemy.types.TypeEngine:
        """Find correct integral column type to fit the given min and max integer values"""

        if min_value is not None and max_value is not None:
            if min_value >= INT16_MIN and max_value <= INT16_MAX:
                return sqlalchemy.sql.sqltypes.SMALLINT()
            if min_value >= INT32_MIN and max_value <= INT32_MAX:
                return sqlalchemy.sql.sqltypes.INTEGER()
            if min_value >= INT64_MIN and max_value <= INT64_MAX:
                return sqlalchemy.sql.sqltypes.BIGINT()
            else:
                num_digits_min = num_digits(min_value)
                num_digits_max = num_digits(max_value)
                digit_count = max(num_digits_min, num_digits_max)
                return self.type_for_fixed_point(precision=digit_count,
                                                 scale=0)
        return super().type_for_integer(min_value, max_value)

    def type_for_fixed_point(self,
                             precision: int,
                             scale: int) -> sqlalchemy.sql.sqltypes.Numeric:
        if precision > 38:
            logger.warning("Using floating point type, as Redshift can't store >38 "
                           f"digits of precision in a fixed point type (requested: {precision})")
            return sqlalchemy.dialects.postgresql.base.DOUBLE_PRECISION()
        return super().type_for_fixed_point(precision=precision,
                                            scale=scale)

    def type_for_floating_point(self,
                                fp_total_bits: int,
                                fp_significand_bits: int) -> sqlalchemy.sql.sqltypes.Numeric:
        if fp_significand_bits > FLOAT64_SIGNIFICAND_BITS:
            logger.warning(f"Falling back to Redshift DOUBLE PRECISION type, as Redshift "
                           "doesn't support fp_significand_bits>{FLOAT64_SIGNIFICAND_BITS} "
                           f"(requested: {fp_significand_bits}")
            return sqlalchemy.sql.sqltypes.Float(precision=FLOAT64_SIGNIFICAND_BITS)
        return super().type_for_floating_point(fp_total_bits=fp_total_bits,
                                               fp_significand_bits=fp_significand_bits)
