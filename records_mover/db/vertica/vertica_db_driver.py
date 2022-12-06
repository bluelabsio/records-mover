from ..driver import DBDriver
import sqlalchemy
from sqlalchemy.sql import text
from records_mover.db.quoting import quote_schema_and_table
from sqlalchemy.schema import Table, Column
import logging
from typing import Optional, Union, Tuple
from ...url.resolver import UrlResolver
from ...url.base import BaseDirectoryUrl
from .loader import VerticaLoader
from ..loader import LoaderFromFileobj, LoaderFromRecordsDirectory
from .unloader import VerticaUnloader
from ..unloader import Unloader
from ...utils.limits import (INT64_MIN, INT64_MAX,
                             FLOAT64_SIGNIFICAND_BITS,
                             num_digits)


logger = logging.getLogger(__name__)


class VerticaDBDriver(DBDriver):
    def __init__(self,
                 db: Union[sqlalchemy.engine.Connection, sqlalchemy.engine.Engine],
                 url_resolver: UrlResolver,
                 s3_temp_base_loc: Optional[BaseDirectoryUrl]=None,
                 **kwargs: object) -> None:
        super().__init__(db)
        self._vertica_loader = VerticaLoader(url_resolver=url_resolver, db=self.db)
        self._vertica_unloader = VerticaUnloader(s3_temp_base_loc=s3_temp_base_loc, db=db)
        self.url_resolver = url_resolver

    def loader(self) -> Optional[LoaderFromRecordsDirectory]:
        return self._vertica_loader

    def loader_from_fileobj(self) -> LoaderFromFileobj:
        return self._vertica_loader

    def unloader(self) -> Optional[Unloader]:
        return self._vertica_unloader

    def has_table(self, schema: str, table: str) -> bool:
        try:
            sql = f"SELECT 1 from {quote_schema_and_table(self.db, schema, table)} limit 0;"
            self.db.execute(sql)
            return True
        except sqlalchemy.exc.ProgrammingError:
            return False

    def schema_sql(self, schema: str, table: str) -> str:
        sql = text("SELECT EXPORT_OBJECTS('', :schema_and_table, false)")
        result = self.db.execute(sql, schema_and_table=f"{schema}.{table}").fetchall()

        if len(result) == 1:
            return result[0].EXPORT_OBJECTS
        else:
            # maybe a permission error?
            return super().schema_sql(schema, table)

    def table(self,
              schema: str,
              table: str) -> Table:
        # sqlalchemy-vertica driver uses system tables that are
        # suuuuper slow in Vertica databases--this is a compromise
        # that pulls column info but not other things
        #
        # https://docs.sqlalchemy.org/en/latest/core/metadata.html#sqlalchemy.schema.Column
        # https://docs.sqlalchemy.org/en/latest/core/reflection.html#
        #    sqlalchemy.engine.reflection.Inspector.get_columns
        columns = [Column(colinfo['name'],
                          type_=colinfo['type'],
                          nullable=colinfo['nullable'],
                          default=colinfo['default'],
                          **colinfo.get('attrs', {}))
                   for colinfo in self.db_engine.dialect.get_columns(self.db_engine,
                                                                     table,
                                                                     schema=schema)]
        t = Table(table, self.meta, schema=schema, *columns)
        return t

    def integer_limits(self,
                       type_: sqlalchemy.types.Integer) ->\
            Optional[Tuple[int, int]]:
        if isinstance(type_, sqlalchemy.sql.sqltypes.INTEGER):
            return (INT64_MIN, INT64_MAX)
        return super().integer_limits(type_)

    def fp_constraints(self,
                       type_: sqlalchemy.types.Float) ->\
            Optional[Tuple[int, int]]:
        if isinstance(type_, sqlalchemy.sql.sqltypes.FLOAT):
            return (64, FLOAT64_SIGNIFICAND_BITS)
        return super().fp_constraints(type_)

    def type_for_integer(self,
                         min_value: Optional[int],
                         max_value: Optional[int]) -> sqlalchemy.types.TypeEngine:
        """Find correct integral column type to fit the given min and max integer values"""

        if min_value is not None and max_value is not None:
            if min_value >= INT64_MIN and max_value <= INT64_MAX:
                return sqlalchemy.sql.sqltypes.INTEGER()
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
            logger.warning("Falling back to Vertica 64-bit FLOAT type, as Vertica doesn't support "
                           f"fp_significand_bits>=54 (requested: {fp_significand_bits}")
            return sqlalchemy.sql.sqltypes.Float(precision=FLOAT64_SIGNIFICAND_BITS)
        return super().type_for_floating_point(fp_total_bits=fp_total_bits,
                                               fp_significand_bits=fp_significand_bits)
