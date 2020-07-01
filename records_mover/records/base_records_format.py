from abc import ABCMeta, abstractmethod
import json
from typing import Mapping, Optional
from .types import RecordsFormatType


class BaseRecordsFormat(metaclass=ABCMeta):
    """
    To create an instance, see :class:`~records_mover.records.ParquetRecordsFormat` or :class:`~records_mover.records.DelimitedRecordsFormat`
    """
    format_type: RecordsFormatType

    def json(self) -> str:
        contents = ''
        contents_data = self.config()
        if contents_data is not None:
            contents = json.dumps(contents_data)
        return contents

    def config(self) -> Optional[Mapping[str, object]]:
        return None

    @abstractmethod
    def generate_filename(self, basename: str) -> str:
        pass
