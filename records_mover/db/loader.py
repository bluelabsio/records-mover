from ..records.records_format import BaseRecordsFormat, DelimitedRecordsFormat
from ..records.types import RecordsFormatType
from abc import ABCMeta, abstractmethod
from typing import Optional, Type
import sqlalchemy


class NegotiatesLoadFormat(metaclass=ABCMeta):
    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        """Return true if the specified format is compatible with the load()
        method"""
        return False

    def best_records_format(self) -> BaseRecordsFormat:
        variant = self.best_records_format_variant('delimited')
        assert variant is not None  # always provided for 'delimited'
        return DelimitedRecordsFormat(variant=variant)

    def best_records_format_variant(self,
                                    records_format_type: RecordsFormatType) -> \
            Optional[str]:
        """Return the most suitable records format delimited variant for
        loading and unloading.  This is generally the format that the
        database unloading and loading natively, which won't require
        translation before loading or after unloading.
        """
        if records_format_type == 'delimited':
            return 'bluelabs'
        else:
            return None


class Loader(NegotiatesLoadFormat):
    def load_failure_exception(self) -> Type[Exception]:
        return sqlalchemy.exc.InternalError
