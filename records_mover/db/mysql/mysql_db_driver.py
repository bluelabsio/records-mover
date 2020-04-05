import sqlalchemy
import sqlalchemy.dialects.mysql
import logging
from ...utils.limits import (INT8_MIN, INT8_MAX,
                             UINT8_MIN, UINT8_MAX,
                             INT16_MIN, INT16_MAX,
                             UINT16_MIN, UINT16_MAX,
                             INT24_MIN, INT24_MAX,
                             UINT24_MIN, UINT24_MAX,
                             INT32_MIN, INT32_MAX,
                             UINT32_MIN, UINT32_MAX,
                             INT64_MIN, INT64_MAX,
                             UINT64_MIN, UINT64_MAX,
                             FLOAT32_SIGNIFICAND_BITS,
                             FLOAT64_SIGNIFICAND_BITS,
                             num_digits)
from ..driver import DBDriver
from typing import Optional, Tuple, Union


logger = logging.getLogger(__name__)


class MySQLDBDriver(DBDriver):
    # https://www.postgresql.org/docs/10/datatype-numeric.html
    def integer_limits(self,
                       type_: sqlalchemy.types.Integer) ->\
            Optional[Tuple[int, int]]:
        if isinstance(type_, sqlalchemy.dialects.mysql.TINYINT):
            return (INT8_MIN, INT8_MAX)
        elif isinstance(type_, sqlalchemy.sql.sqltypes.SMALLINT):
            return (INT16_MIN, INT16_MAX)
        elif isinstance(type_, sqlalchemy.dialects.mysql.MEDIUMINT):
            return (INT24_MIN, INT24_MAX)
        elif isinstance(type_, sqlalchemy.sql.sqltypes.INTEGER):
            return (INT32_MIN, INT32_MAX)
        elif isinstance(type_, sqlalchemy.sql.sqltypes.BIGINT):
            return (INT64_MIN, INT64_MAX)
        return super().integer_limits(type_)

    def fp_constraints(self,
                       type_: sqlalchemy.types.Float) ->\
            Optional[Tuple[int, int]]:
        if isinstance(type_, sqlalchemy.dialects.mysql.DOUBLE):
            return (64, FLOAT64_SIGNIFICAND_BITS)
        elif isinstance(type_, sqlalchemy.sql.sqltypes.FLOAT):
            return (32, FLOAT32_SIGNIFICAND_BITS)
        return super().fp_constraints(type_)

    def type_for_integer(self,
                         min_value: Optional[int],
                         max_value: Optional[int]) -> sqlalchemy.types.TypeEngine:
        """Find correct integral column type to fit the given min and max integer values"""

        if min_value is not None and max_value is not None:
            pass
            if min_value >= INT8_MIN and max_value <= INT8_MAX:
                return sqlalchemy.dialects.mysql.TINYINT()
            elif min_value >= UINT8_MIN and max_value <= UINT8_MAX:
                return sqlalchemy.dialects.mysql.TINYINT(unsigned=True)
            elif min_value >= INT16_MIN and max_value <= INT16_MAX:
                return sqlalchemy.sql.sqltypes.SMALLINT()
            elif min_value >= UINT16_MIN and max_value <= UINT16_MAX:
                return sqlalchemy.dialects.mysql.SMALLINT(unsigned=True)
            elif min_value >= INT32_MIN and max_value <= INT32_MAX:
                return sqlalchemy.sql.sqltypes.INTEGER()
            elif min_value >= UINT32_MIN and max_value <= UINT32_MAX:
                return sqlalchemy.dialects.mysql.INTEGER(unsigned=True)
            elif min_value >= INT64_MIN and max_value <= INT64_MAX:
                return sqlalchemy.sql.sqltypes.BIGINT()
            elif min_value >= UINT64_MIN and max_value <= UINT64_MAX:
                return sqlalchemy.dialects.mysql.BIGINT(unsigned=True)
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
            logger.warning(f"Falling back to Postgres DOUBLE type, as MySQL "
                           "doesn't support fp_significand_bits>{FLOAT64_SIGNIFICAND_BITS} "
                           f"(requested: {fp_significand_bits}")
            return sqlalchemy.sql.sqltypes.Float(precision=FLOAT64_SIGNIFICAND_BITS)
        return super().type_for_floating_point(fp_total_bits=fp_total_bits,
                                               fp_significand_bits=fp_significand_bits)

    def type_for_fixed_point(self,
                             precision: int,
                             scale: int) -> sqlalchemy.sql.sqltypes.Numeric:
        # BigQuery NUMERIC() type takes no arguments and supports 38
        # digits of precision, of which 9 digits are scale.
        if precision > 65:
            logger.warning('Using MySQL DOUBLE type to represent '
                           f'NUMERIC({precision},{scale}))')
            return sqlalchemy.dialects.mysql.DOUBLE()
        else:
            return super().type_for_fixed_point(precision=precision,
                                                scale=scale)

    def varchar_length_is_in_chars(self) -> bool:
        # This is assuming folks are using MySQL 5+
        # https://stackoverflow.com/questions/1997540/mysql-varchar-lengths-and-utf-8
        return True

    def type_for_date_plus_time(self, has_tz: bool = False) -> sqlalchemy.sql.sqltypes.DateTime:
        # Support six digits of fractional seconds to match other
        # databases and general expectations for a datetime
        #
        # Never has timezone, as the one type with a timezone
        # (TIMESTAMP) doesn't allow for dates before Jan 1, 1970, so
        # it's not generally useful.
        return sqlalchemy.dialects.mysql.DATETIME(fsp=6)
