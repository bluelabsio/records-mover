from contextlib import contextmanager
from sqlalchemy import MetaData
from ..records.unload_plan import RecordsUnloadPlan
from ..records.records_format import BaseRecordsFormat
from ..records.records_directory import RecordsDirectory
from records_mover.url.base import BaseDirectoryUrl
from typing import Union, List, Optional, Iterator
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
        """Export data in the specified table to the RecordsDirectory instance
        named 'directory'.  Guarantees a manifest file named
        'manifest' is written to the target directory pointing to the
        target records.

        Returns number of rows loaded (if database provides that
        info)."""
        ...

    @abstractmethod
    def known_supported_records_formats_for_unload(self) -> List[BaseRecordsFormat]:
        """Supplies a list of the records formats which can be bulk exported
        from this database.  This may not be the full set - see
        can_unload_this_format() to test other possibilities.
        """
        ...

    @abstractmethod
    def can_unload_format(self, target_records_format: BaseRecordsFormat) -> bool:
        """Determine whether a specific records format can be exported by this
        database."""
        ...

    @abstractmethod
    def can_unload_to_scheme(self, scheme: str) -> bool:
        """Determine whether a URL scheme can be unloaded to by this database.
        Depending on capabilities of this driver and the underlying database,
        this may depend on whether a temporary location has been configured."""
        ...

    @abstractmethod
    @contextmanager
    def temporary_unloadable_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        """Provide a temporary directory which can be used for bulk export from
        this database and clean it up when done"""
        ...

    def best_records_format(self) -> Optional[BaseRecordsFormat]:
        """Returns the ideal records format for export by this database.
        Useful in the absence of other constraints."""
        supported_formats = self.known_supported_records_formats_for_unload()
        if len(supported_formats) == 0:
            return None
        return supported_formats[0]
