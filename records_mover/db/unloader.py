from sqlalchemy import MetaData
from ..records.unload_plan import RecordsUnloadPlan
from ..records.records_format import BaseRecordsFormat
from ..records.records_directory import RecordsDirectory
from typing import Union, List, Optional
from abc import ABCMeta, abstractmethod
import sqlalchemy


class Unloader(metaclass=ABCMeta):
    def __init__(self,
                 db: Union[sqlalchemy.engine.Connection, sqlalchemy.engine.Engine]) -> None:
        self.db = db
        self.meta = MetaData()

    @abstractmethod
    def unload(self,
               schema: str,
               table: str,
               unload_plan: RecordsUnloadPlan,
               directory: RecordsDirectory) -> Optional[int]:
        ...

    @abstractmethod
    def known_supported_records_formats_for_unload(self) -> List[BaseRecordsFormat]:
        ...

    @abstractmethod
    def can_unload_this_format(self, target_records_format: BaseRecordsFormat) -> bool:
        ...

    def best_records_format(self) -> Optional[BaseRecordsFormat]:
        supported_formats = self.known_supported_records_formats_for_unload()
        if len(supported_formats) == 0:
            return None
        return supported_formats[0]
