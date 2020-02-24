from sqlalchemy import Column
from sqlalchemy.types import TypeEngine
import datetime
import numpy as np
from ...processing_instructions import ProcessingInstructions
import logging
from typing import Optional, Dict, Any, Type, cast, Union, TYPE_CHECKING
from ....utils.limits import (INT8_MIN, INT8_MAX,
                              UINT8_MIN, UINT8_MAX,
                              INT16_MIN, INT16_MAX,
                              UINT16_MIN, UINT16_MAX,
                              INT32_MIN, INT32_MAX,
                              UINT32_MIN, UINT32_MAX,
                              INT64_MIN, INT64_MAX,
                              UINT64_MIN, UINT64_MAX,
                              FLOAT16_SIGNIFICAND_BITS,
                              FLOAT32_SIGNIFICAND_BITS,
                              FLOAT64_SIGNIFICAND_BITS,
                              FLOAT80_SIGNIFICAND_BITS)
from .representation import RecordsSchemaFieldRepresentation
from .constraints import (RecordsSchemaFieldConstraints,
                          RecordsSchemaFieldIntegerConstraints,
                          RecordsSchemaFieldDecimalConstraints)
from .statistics import RecordsSchemaFieldStatistics
from .types import RECORDS_FIELD_TYPES
from .sqlalchemy import (field_from_sqlalchemy_column,
                         field_to_sqlalchemy_type,
                         field_to_sqlalchemy_column)
if TYPE_CHECKING:
    from pandas import Series, Index
    from records_mover.db import DBDriver  # noqa
    from .types import FieldType

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
                           rows_sampled: int) -> None:
        from .pandas import refine_field_from_series
        refine_field_from_series(self, series, total_rows, rows_sampled)

    @staticmethod
    def is_more_specific_type(a: 'FieldType', b: 'FieldType') -> bool:
        if b == 'string' and a != 'string':
            return True
        return False

    @staticmethod
    def python_type_to_field_type(specific_type: Type[Any]) -> Optional['FieldType']:
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
    def from_sqlalchemy_column(column: Column,
                               driver: 'DBDriver',
                               rep_type: str)\
            -> 'RecordsSchemaField':
        return field_from_sqlalchemy_column(column=column,
                                            driver=driver,
                                            rep_type=rep_type)

    def to_sqlalchemy_type(self,
                           driver: 'DBDriver') -> TypeEngine:
        return field_to_sqlalchemy_type(self, driver)

    def to_sqlalchemy_column(self, driver: 'DBDriver') -> Column:
        return field_to_sqlalchemy_column(self, driver)

    def to_numpy_dtype(self) -> Union[Type[Any], str]:
        if self.field_type == 'integer':
            int_constraints =\
                cast(Optional[RecordsSchemaFieldIntegerConstraints], self.constraints)
            min_: Optional[int] = None
            max_: Optional[int] = None
            if int_constraints:
                min_ = int_constraints.min_
                max_ = int_constraints.max_

            if min_ is not None and max_ is not None:
                if min_ >= INT8_MIN and max_ <= INT8_MAX:
                    return np.int8
                elif min_ >= UINT8_MIN and max_ <= UINT8_MAX:
                    return np.uint8
                elif min_ >= INT16_MIN and max_ <= INT16_MAX:
                    return np.int16
                elif min_ >= UINT16_MIN and max_ <= UINT16_MAX:
                    return np.uint16
                elif min_ >= INT32_MIN and max_ <= INT32_MAX:
                    return np.int32
                elif min_ >= UINT32_MIN and max_ <= UINT32_MAX:
                    return np.uint32
                elif min_ >= INT64_MIN and max_ <= INT64_MAX:
                    return np.int64
                elif min_ >= UINT64_MIN and max_ <= UINT64_MAX:
                    return np.uint64
                else:
                    logger.warning("Asked for a type larger than int64 in dataframe "
                                   f"field '{self.name}' - providing float128, but "
                                   "loss of precision will occur!  "
                                   f"Requested min/max values: {min_}/{max_}")
                    return np.float128
            else:
                logger.warning(f"No integer constraints provided for field '{self.name}'; "
                               "using int64")
                return np.int64
            # return driver.type_for_integer(min_=min_, max_=max_)
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

    def convert_datetime_to_datetimetz(self) -> 'RecordsSchemaField':
        field_type = self.field_type
        if field_type == 'datetime':
            field_type = 'datetimetz'

        return RecordsSchemaField(name=self.name,
                                  field_type=field_type,
                                  constraints=self.constraints,
                                  statistics=self.statistics,
                                  representations=self.representations)
