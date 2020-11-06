import logging
from sqlalchemy import Column
import sqlalchemy
from typing import cast, Union, Optional, Type, TYPE_CHECKING
from .constraints import (RecordsSchemaFieldConstraints,
                          RecordsSchemaFieldIntegerConstraints,
                          RecordsSchemaFieldDecimalConstraints,
                          RecordsSchemaFieldStringConstraints)
from .representation import RecordsSchemaFieldRepresentation
from .statistics import RecordsSchemaFieldStringStatistics
from .string_length_generator import generate_string_length
if TYPE_CHECKING:
    from ....db import DBDriver  # noqa
    from ..field import RecordsSchemaField  # noqa
    from ..schema import RecordsSchema  # noqa
    from .field_types import FieldType  # noqa


logger = logging.getLogger(__name__)


def field_from_sqlalchemy_column(column: Column,
                                 driver: 'DBDriver',
                                 rep_type: str)\
            -> 'RecordsSchemaField':
    from ..field import RecordsSchemaField  # noqa
    name = column.name
    type_: Union[Type[sqlalchemy.types.TypeEngine],
                 sqlalchemy.types.TypeEngine] = column.type
    constraints: Optional[RecordsSchemaFieldConstraints] = None
    field_type = None  # type: Optional[FieldType]

    required = not column.nullable

    # We don't yet try to pull information about uniqueness
    # constraints out of database tables, so set this to null,
    # which means "we don't know":
    #
    # https://github.com/bluelabsio/records-mover/issues/90
    unique = None

    if isinstance(type_, sqlalchemy.sql.sqltypes.Integer):
        field_type = 'integer'
        constraints = RecordsSchemaFieldIntegerConstraints.\
            from_sqlalchemy_type(required=required,
                                 unique=unique,
                                 type_=type_,
                                 driver=driver)
    elif (isinstance(type_, (sqlalchemy.sql.sqltypes.Numeric,
                             sqlalchemy.sql.sqltypes.Float))):
        field_type = 'decimal'
        constraints = RecordsSchemaFieldDecimalConstraints.\
            from_sqlalchemy_type(required=required,
                                 unique=unique,
                                 type_=type_,
                                 driver=driver)
    elif (isinstance(type_, sqlalchemy.sql.sqltypes.String)):
        field_type = 'string'
        constraints = RecordsSchemaFieldStringConstraints.\
            from_sqlalchemy_type(required=required,
                                 unique=unique,
                                 type_=type_,
                                 driver=driver)
    elif isinstance(type_, sqlalchemy.sql.sqltypes.Date):
        field_type = 'date'
    elif isinstance(type_, sqlalchemy.sql.sqltypes.DateTime):
        date_plus_time_with_timezone = driver.type_for_date_plus_time(has_tz=True)
        date_plus_time_no_timezone = driver.type_for_date_plus_time(has_tz=False)

        #
        # Databases are super annoying about these.
        #
        # BigQuery's SQLAlchemy driver uses TIMEZONE for the
        # former, DATETIME for the latter, without setting the
        # 'timezone' variable.
        #
        # Redshift and Vertica use DateTime for both with the
        # 'timezone' variable set appropriately.
        #
        if type(date_plus_time_with_timezone) != type(date_plus_time_no_timezone):
            if isinstance(type_, type(date_plus_time_with_timezone)):
                field_type = 'datetimetz'
            elif isinstance(type_, type(date_plus_time_no_timezone)):
                field_type = 'datetime'
            else:
                raise NotImplementedError(f"Teach me how to handle {type_}")
        else:
            if type_.timezone:
                field_type = 'datetimetz'
            else:
                field_type = 'datetime'
    elif isinstance(type_, sqlalchemy.sql.sqltypes.Time):
        if type_.timezone:
            field_type = 'timetz'
        else:
            field_type = 'time'
    elif isinstance(type_, sqlalchemy.sql.sqltypes.Boolean):
        field_type = 'boolean'
    else:
        raise NotImplementedError("Teach me how to handle this SQLAlchemy type: "
                                  f"{type(type_)}")
    if constraints is None:
        constraints = RecordsSchemaFieldConstraints.\
            from_sqlalchemy_type(required=required,
                                 unique=unique,
                                 type_=type_,
                                 driver=driver)
    representations = {
        'origin': RecordsSchemaFieldRepresentation.
        from_sqlalchemy_column(column, driver.db.dialect,
                               rep_type)
    }

    # We don't currently gather statistics from databases - which
    # can bite us when exporting from BigQuery, for instance:
    #
    # https://github.com/bluelabsio/records-mover/issues/91
    statistics = None

    return RecordsSchemaField(name=name,
                              field_type=field_type,
                              constraints=constraints,
                              statistics=statistics,
                              representations=representations)


def field_to_sqlalchemy_type(field: 'RecordsSchemaField',
                             driver: 'DBDriver') -> sqlalchemy.types.TypeEngine:
    if field.field_type == 'integer':
        if field.constraints and\
           not isinstance(field.constraints, RecordsSchemaFieldIntegerConstraints):
            raise ValueError(f"Incorrect constraint type in {field.name}: {field.constraints}")

        int_constraints =\
            cast(Optional[RecordsSchemaFieldIntegerConstraints], field.constraints)
        min_: Optional[int] = None
        max_: Optional[int] = None
        if int_constraints:
            min_ = int_constraints.min_
            max_ = int_constraints.max_
        return driver.type_for_integer(min_value=min_, max_value=max_)
    elif field.field_type == 'decimal':
        decimal_constraints =\
            cast(Optional[RecordsSchemaFieldDecimalConstraints], field.constraints)
        if decimal_constraints:
            if (decimal_constraints.fixed_precision is not None and
               decimal_constraints.fixed_scale is not None):
                return driver.type_for_fixed_point(precision=decimal_constraints.fixed_precision,
                                                   scale=decimal_constraints.fixed_scale)

            if (decimal_constraints.fp_total_bits is not None and
               decimal_constraints.fp_significand_bits is not None):
                return driver.\
                    type_for_floating_point(fp_total_bits=decimal_constraints.fp_total_bits,
                                            fp_significand_bits=decimal_constraints.
                                            fp_significand_bits)

        # default to an IEEE double absent more information:
        #
        # https://en.wikipedia.org/wiki/Double-precision_floating-point_format
        return driver.type_for_floating_point(fp_total_bits=64,
                                              fp_significand_bits=53)

    elif field.field_type == 'boolean':
        return sqlalchemy.sql.sqltypes.BOOLEAN()
    elif field.field_type == 'string':
        if field.constraints and\
           not isinstance(field.constraints, RecordsSchemaFieldStringConstraints):
            raise ValueError(f"Incorrect constraint type in {field.name}: {field.constraints}")

        if field.statistics and\
           not isinstance(field.statistics, RecordsSchemaFieldStringStatistics):
            raise ValueError(f"Incorrect statistics type in {field.name}: {field.statistics}")

        string_constraints =\
            cast(Optional[RecordsSchemaFieldStringConstraints], field.constraints)
        string_statistics =\
            cast(Optional[RecordsSchemaFieldStringStatistics], field.statistics)
        n = generate_string_length(string_constraints, string_statistics, driver)
        return sqlalchemy.sql.sqltypes.String(n)
    elif field.field_type == 'date':
        return sqlalchemy.sql.sqltypes.DATE()
    elif field.field_type == 'datetime' or field.field_type == 'datetimetz':
        has_tz = field.field_type == 'datetimetz'
        return driver.type_for_date_plus_time(has_tz=has_tz)
    elif field.field_type == 'time' or field.field_type == 'timetz':
        if driver.supports_time_type():
            has_tz = field.field_type == 'timetz'
            return sqlalchemy.sql.sqltypes.TIME(timezone=has_tz)
        else:
            # HH:MM AM; should be same in bytes as chars at least in UTF-8
            return sqlalchemy.sql.sqltypes.VARCHAR(8)
    else:
        raise NotImplementedError("Teach me how to handle records schema "
                                  f"type {field.field_type}")


def field_to_sqlalchemy_column(field: 'RecordsSchemaField', driver: 'DBDriver') -> Column:
    name = field.name if driver is None else driver.make_column_name_valid(field.name)
    return Column(name,
                  type_=field.to_sqlalchemy_type(driver),
                  nullable=field.constraints is None or not field.constraints.required)
