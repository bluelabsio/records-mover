import sqlalchemy
import logging
from records_mover.url.resolver import UrlResolver
from records_mover.utils.limits import (INT16_MIN, INT16_MAX,
                                        INT32_MIN, INT32_MAX,
                                        INT64_MIN, INT64_MAX,
                                        FLOAT32_SIGNIFICAND_BITS,
                                        FLOAT64_SIGNIFICAND_BITS,
                                        num_digits)
from ..driver import DBDriver
from .loader import PostgresLoader
from ..loader import LoaderFromFileobj, LoaderFromRecordsDirectory
from .unloader import PostgresUnloader
from ..unloader import Unloader
from typing import Optional, Tuple, Union


logger = logging.getLogger(__name__)


class PostgresDBDriver(DBDriver):
    def __init__(self,
                 db: Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection],
                 url_resolver: UrlResolver,
                 **kwargs) -> None:
        super().__init__(db)
        self._postgres_loader = PostgresLoader(url_resolver=url_resolver,
                                               meta=self.meta,
                                               db=self.db)
        self._postgres_unloader = PostgresUnloader(db=self.db)

    def loader(self) -> Optional[LoaderFromRecordsDirectory]:
        return self._postgres_loader

    def loader_from_fileobj(self) -> LoaderFromFileobj:
        return self._postgres_loader

    def unloader(self) -> Optional[Unloader]:
        return self._postgres_unloader

    # https://www.postgresql.org/docs/10/datatype-numeric.html
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

    def type_for_floating_point(self,
                                fp_total_bits: int,
                                fp_significand_bits: int) -> sqlalchemy.sql.sqltypes.Numeric:
        if fp_significand_bits > FLOAT64_SIGNIFICAND_BITS:
            logger.warning(f"Falling back to Postgres DOUBLE PRECISION type, as Postgres "
                           "doesn't support fp_significand_bits>{FLOAT64_SIGNIFICAND_BITS} "
                           f"(requested: {fp_significand_bits}")
            return sqlalchemy.sql.sqltypes.Float(precision=FLOAT64_SIGNIFICAND_BITS)
        return super().type_for_floating_point(fp_total_bits=fp_total_bits,
                                               fp_significand_bits=fp_significand_bits)
