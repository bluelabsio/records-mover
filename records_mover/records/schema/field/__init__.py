import datetime
from ...processing_instructions import ProcessingInstructions
import logging
from typing import Optional, Dict, Any, Type, cast, TYPE_CHECKING
from ....utils.limits import (FLOAT16_SIGNIFICAND_BITS,
                              FLOAT32_SIGNIFICAND_BITS,
                              FLOAT64_SIGNIFICAND_BITS,
                              FLOAT80_SIGNIFICAND_BITS)
from .representation import RecordsSchemaFieldRepresentation
from .constraints import (RecordsSchemaFieldConstraints,
                          RecordsSchemaFieldIntegerConstraints,
                          RecordsSchemaFieldDecimalConstraints)
from .statistics import RecordsSchemaFieldStatistics
from .field_types import RECORDS_FIELD_TYPES
if TYPE_CHECKING:
    from pandas import Series, Index
    from sqlalchemy import Column
    from sqlalchemy.types import TypeEngine
    from records_mover.db import DBDriver  # noqa
    from .field_types import FieldType
    from .pandas import Dtype

    from mypy_extensions import TypedDict

    class MandatoryFieldDict(TypedDict):
        type: FieldType

    from .statistics import FieldStatisticsDict
    from .constraints import FieldConstraintsDict
    from .representation import FieldRepresentationDict

    class FieldDict(MandatoryFieldDict, total=False):
        index: int
        constraints: FieldConstraintsDict
        statistics: FieldStatisticsDict
        representations: Dict[str, FieldRepresentationDict]

logger = logging.getLogger(__name__)

DEFAULT_COLUMN_LENGTH = 256


class RecordsSchemaField:
    def __init__(self,
                 name,
                 field_type,
                 constraints,
                 statistics,
                 representations):
        # type: (str, FieldType, Optional[RecordsSchemaFieldConstraints], Optional[RecordsSchemaFieldStatistics], Dict[str, RecordsSchemaFieldRepresentation]) -> None  # noqa
        self.name = name
        self.field_type = field_type
        self.constraints = constraints
        self.statistics = statistics
        self.representations = representations

    def refine_from_series(self,
                           series: 'Series',
                           total_rows: int,
                           rows_sampled: int) -> 'RecordsSchemaField':
        from .pandas import refine_field_from_series
        return refine_field_from_series(self, series, total_rows, rows_sampled)

    @staticmethod
    def is_more_specific_type(a: 'FieldType', b: 'FieldType') -> bool:
        if b == 'string' and a != 'string':
            return True
        return False

    @staticmethod
    def python_type_to_field_type(specific_type: Type[Any]) -> Optional['FieldType']:
        import numpy as np
        import pandas as pd

        # Note: records spec doesn't cover complex number types, so
        # np.complex_, complex64 and complex128 are not supported
        # except as a string.
        type_mapping: Dict[Type[Any], 'FieldType'] = {
            # https://docs.scipy.org/doc/numpy-1.14.0/user/basics.types.html
            int: 'integer',
            np.int64: 'integer',
            np.int_: 'integer',
            np.intc: 'integer',
            np.intp: 'integer',
            np.int8: 'integer',
            np.int16: 'integer',
            np.int32: 'integer',
            np.int64: 'integer',
            np.uint8: 'integer',
            np.uint16: 'integer',
            np.uint32: 'integer',
            np.uint64: 'integer',

            np.float_: 'decimal',
            np.float16: 'decimal',
            np.float32: 'decimal',
            np.float64: 'decimal',
            np.float128: 'decimal',
            float: 'decimal',

            str: 'string',
            np.str_: 'string',
            np.object_: 'string',

            bool: 'boolean',
            np.bool_: 'boolean',

            datetime.date: 'date',

            datetime.time: 'time',

            datetime.datetime: 'datetime',

            pd.Timestamp: 'datetime',
        }
        if specific_type not in type_mapping:
            logger.warning(f"Please teach me how to map {specific_type} into records "
                           "schema field types")
        return type_mapping.get(specific_type)

    @staticmethod
    def from_index(index: 'Index',
                   processing_instructions: ProcessingInstructions) -> 'RecordsSchemaField':
        from .pandas import field_from_index
        return field_from_index(index=index,
                                processing_instructions=processing_instructions)

    @staticmethod
    def from_series(series: 'Series',
                    processing_instructions: ProcessingInstructions) -> 'RecordsSchemaField':
        from .pandas import field_from_series
        return field_from_series(series=series,
                                 processing_instructions=processing_instructions)

    @staticmethod
    def from_sqlalchemy_column(column: 'Column',
                               driver: 'DBDriver',
                               rep_type: str)\
            -> 'RecordsSchemaField':
        from .sqlalchemy import field_from_sqlalchemy_column

        return field_from_sqlalchemy_column(column=column,
                                            driver=driver,
                                            rep_type=rep_type)

    def to_sqlalchemy_type(self,
                           driver: 'DBDriver') -> 'TypeEngine':
        from .sqlalchemy import field_to_sqlalchemy_type

        return field_to_sqlalchemy_type(self, driver)

    def to_sqlalchemy_column(self, driver: 'DBDriver') -> 'Column':
        from .sqlalchemy import field_to_sqlalchemy_column

        return field_to_sqlalchemy_column(self, driver)

    def cast_series_type(self, series: 'Series') -> 'Series':
        import pandas as pd
        if self.field_type == 'time':
            if series.size > 0:
                # https://stackoverflow.com/questions/34501930/how-to-convert-timedelta-to-time-of-day-in-pandas
                #
                # Some databases (e.g., MySQL) contains a TIME type
                # which is ambiguous - it can either represent a
                # particular time of day or it can represent an
                # elapsed amount of time.
                #
                # Clever, right?
                #
                # Unfortunately, Pandas knows about time deltas, but
                # not about times of day, so upon use of read_sql(),
                # these objects will come out as as a timedelta64[ns]
                # type.
                #
                # Since that's not what our 'time' field type means,
                # we have to convert it back to a string, or when it
                # gets turned into a CSV later, it'll look really
                # goofy - 1pm will come out as: "0 days 01:00:00".
                #
                if type(series[0]) == pd.Timedelta:
                    # https://stackoverflow.com/questions/34501930/how-to-convert-timedelta-to-time-of-day-in-pandas

                    # Convert from "0 days 12:34:56.000000000" to "12:34:56"
                    def components_to_time_str(df: pd.DataFrame) -> datetime.time:
                        return datetime.time(hour=df['hours'],
                                             minute=df['minutes'],
                                             second=df['seconds'])
                    logger.debug("Applying pd.Timedelta logic on series for %s", self.name)
                    out = series.dt.components.apply(axis=1, func=components_to_time_str)
                    return out

        target_type = self.to_pandas_dtype()
        logger.debug("Casting field %s from type %r to type %s", self.name, series.dtype,
                     target_type)
        return series.astype(target_type)

    def to_pandas_dtype(self) -> 'Dtype':
        import numpy as np
        import pandas as pd
        from .pandas import supports_nullable_ints, integer_type_for_range

        has_extension_types = supports_nullable_ints()

        if self.field_type == 'integer':
            int_constraints =\
                cast(Optional[RecordsSchemaFieldIntegerConstraints], self.constraints)
            min_: Optional[int] = None
            max_: Optional[int] = None
            required = False
            if int_constraints:
                min_ = int_constraints.min_
                max_ = int_constraints.max_
                required = int_constraints.required

            if not required and not has_extension_types:
                logger.warning(f"Dataframe field {self.name} is nullable, but using pandas "
                               f"{pd.__version__} which does not support nullable integer type")

            if min_ is not None and max_ is not None:
                dtype = integer_type_for_range(min_, max_, has_extension_types)
                if dtype:
                    return dtype
                else:
                    logger.warning("Asked for a type larger than int64 in dataframe "
                                   f"field '{self.name}' - providing float128, but "
                                   "loss of precision will occur!  "
                                   f"Requested min/max values: {min_}/{max_}")
                    return np.float128
            else:
                logger.warning(f"No integer constraints provided for field '{self.name}'; "
                               "using int64")
                if has_extension_types:
                    return pd.Int64Dtype()
                else:
                    return np.int64
        elif self.field_type == 'decimal':
            decimal_constraints =\
                cast(Optional[RecordsSchemaFieldDecimalConstraints], self.constraints)
            if decimal_constraints:
                if (decimal_constraints.fixed_precision is not None and
                   decimal_constraints.fixed_scale is not None):
                    logger.warning("Pandas doesn't support a fixed precision type - "
                                   "using np.float64")
                    return np.float64
                elif (decimal_constraints.fp_total_bits is not None and
                      decimal_constraints.fp_significand_bits is not None):
                    if (decimal_constraints.fp_total_bits <= 16 and
                       decimal_constraints.fp_significand_bits <= FLOAT16_SIGNIFICAND_BITS):
                        return np.float16
                    elif (decimal_constraints.fp_total_bits <= 32 and
                          decimal_constraints.fp_significand_bits <= FLOAT32_SIGNIFICAND_BITS):
                        return np.float32
                    elif (decimal_constraints.fp_total_bits <= 64 and
                          decimal_constraints.fp_significand_bits <= FLOAT64_SIGNIFICAND_BITS):
                        return np.float64
                    elif (decimal_constraints.fp_total_bits <= 80 and
                          decimal_constraints.fp_significand_bits <= FLOAT80_SIGNIFICAND_BITS):
                        return np.float128
                    else:
                        logger.warning("Downgrading float type to np.float128.  "
                                       "Requested total bits: "
                                       f"{decimal_constraints.fp_total_bits}.  "
                                       "Requested significand bits: "
                                       f"{decimal_constraints.fp_significand_bits}")
                        return np.float128

            logger.warning(f"No decimal constraints provided for field '{self.name}'; "
                           "using float64")
            return np.float64
        elif self.field_type == 'boolean':
            return np.bool_
        elif self.field_type == 'string':
            return np.object_
        elif self.field_type == 'date':
            return np.object_
        elif self.field_type == 'datetime':
            return 'datetime64[ns]'
        elif self.field_type == 'datetimetz':
            return 'datetime64[ns, UTC]'
        elif self.field_type == 'time':
            return np.object_
        elif self.field_type == 'timetz':
            return np.object_
        else:
            raise NotImplementedError("Teach me how to handle records schema "
                                      f"type {self.field_type}")

    def to_data(self) -> 'FieldDict':
        out: 'FieldDict' = {'type': self.field_type}
        if self.constraints:
            out['constraints'] = self.constraints.to_data()
        if self.statistics:
            out['statistics'] = self.statistics.to_data()
        if self.representations:
            out['representations'] = {
                system: rep.to_data()
                for system, rep in self.representations.items()
            }
        return out

    @staticmethod
    def from_data(name: str, data: 'FieldDict') -> 'RecordsSchemaField':
        representations = {}
        field_type = data['type']
        if field_type not in RECORDS_FIELD_TYPES:
            logger.warning(f"Unknown field_type ({field_type})--consider upgrading records-mover")
        for rep_name, rep_data in data.get('representations', {}).items():
            field_representation = RecordsSchemaFieldRepresentation.from_data(rep_data)
            if field_representation is not None:
                representations[rep_name] = field_representation
        return RecordsSchemaField(name=name,
                                  field_type=data['type'],
                                  constraints=RecordsSchemaFieldConstraints.
                                  from_data(data.get('constraints'), field_type=field_type),
                                  statistics=RecordsSchemaFieldStatistics.
                                  from_data(data.get('statistics'), field_type=field_type),
                                  representations=representations)

    def cast(self, field_type: 'FieldType') -> 'RecordsSchemaField':
        if self.field_type == field_type:
            return self
        if self.constraints is None:
            constraints = None
        else:
            constraints = self.constraints.cast(field_type)
        if self.statistics is None:
            statistics = None
        else:
            statistics = self.statistics.cast(field_type)
        field = RecordsSchemaField(name=self.name,
                                   field_type=field_type,
                                   constraints=constraints,
                                   statistics=statistics,
                                   representations=self.representations)
        return field
