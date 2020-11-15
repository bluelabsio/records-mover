from records_mover.records.targets.base import (
    SupportsMoveFromRecordsDirectory,
    MightSupportMoveFromTempLocAfterFillingIt,
    MightSupportMoveFromFileobjsSource,
    SupportsMoveFromDataframes,
)
from sqlalchemy.engine import Engine, Connection
from records_mover.records.prep import TablePrep, TargetTableDetails
from records_mover.records.records_format import BaseRecordsFormat
from records_mover.db import DBDriver
from records_mover.records.records_directory import RecordsDirectory
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.existing_table_handling import ExistingTableHandling
from records_mover.records.results import MoveResult
from records_mover.records.sources import SupportsMoveToRecordsDirectory
from records_mover.records.sources.fileobjs import FileobjsSource
from records_mover.records.targets.table.move_from_records_directory import (
    DoMoveFromRecordsDirectory
)
from records_mover.records.targets.table.move_from_fileobjs_source import DoMoveFromFileobjsSource
from records_mover.records.targets.table.move_from_temp_loc_after_filling_it import (
    DoMoveFromTempLocAfterFillingIt
)
import logging
from typing import Callable, Union, Optional, Dict, List, TYPE_CHECKING
if TYPE_CHECKING:
    from records_mover.records.sources.dataframes import DataframesRecordsSource


logger = logging.getLogger(__name__)


class TableRecordsTarget(SupportsMoveFromRecordsDirectory,
                         MightSupportMoveFromTempLocAfterFillingIt,
                         MightSupportMoveFromFileobjsSource,
                         SupportsMoveFromDataframes,
                         TargetTableDetails):
    def __init__(self,
                 schema_name: str,
                 table_name: str,
                 db_engine: Engine,
                 db_driver: Callable[[Union[Engine, Connection]], DBDriver],
                 add_user_perms_for: Optional[Dict[str, List[str]]] = None,
                 add_group_perms_for: Optional[Dict[str, List[str]]] = None,
                 existing_table_handling: ExistingTableHandling =
                 ExistingTableHandling.DELETE_AND_OVERWRITE,
                 drop_and_recreate_on_load_error: bool = False) -> None:
        self.schema_name = schema_name
        self.table_name = table_name
        self.db_driver = db_driver  # type: ignore
        self.db_engine = db_engine
        self.add_user_perms_for = add_user_perms_for
        self.add_group_perms_for = add_group_perms_for
        self.existing_table_handling = existing_table_handling
        self.drop_and_recreate_on_load_error = drop_and_recreate_on_load_error
        self.prep = TablePrep(self)
        # advertise what format we prefer to be given for mover paths
        # that don't yet support full records negotiation.
        #
        # https://github.com/bluelabsio/records-mover/issues/88
        self.records_format = next(iter(self.known_supported_records_formats()), None)

    def move_from_records_directory(self,
                                    directory: RecordsDirectory,
                                    processing_instructions: ProcessingInstructions,
                                    override_records_format: Optional[BaseRecordsFormat] = None)\
            -> MoveResult:
        return DoMoveFromRecordsDirectory(self.prep,
                                          self,
                                          directory,
                                          processing_instructions,
                                          override_records_format).move()

    def move_from_fileobjs_source(self,
                                  fileobjs_source: FileobjsSource,
                                  processing_instructions: ProcessingInstructions) -> MoveResult:
        return DoMoveFromFileobjsSource(self.prep,
                                        self,
                                        fileobjs_source,
                                        processing_instructions).move()

    def can_move_from_fileobjs_source(self) -> bool:
        driver = self.db_driver(self.db_engine)
        loader = driver.loader_from_fileobj()
        return loader is not None

    def can_move_directly_from_scheme(self, scheme: str) -> bool:
        driver = self.db_driver(self.db_engine)
        loader = driver.loader()
        if loader is None:
            # can't bulk load at all, so can't load direct!
            logger.warning(f"No loader configured for this database type ({self.db_engine.name})")
            return False
        return loader.best_scheme_to_load_from() == scheme

    def known_supported_records_formats(self) -> List[BaseRecordsFormat]:
        driver = self.db_driver(self.db_engine)
        loader = driver.loader()
        if loader is None:
            logger.warning(f"No loader configured for this database type ({self.db_engine.name})")
            return []
        return loader.known_supported_records_formats_for_load()

    def can_move_from_format(self,
                             source_records_format: BaseRecordsFormat) -> bool:
        """Return true if writing the specified format satisfies our format
        needs"""
        driver = self.db_driver(self.db_engine)
        loader = driver.loader()
        if loader is None:
            logger.warning(f"No loader configured for this database type ({self.db_engine.name})")
            return False
        return loader.can_load_this_format(source_records_format)

    def can_move_from_temp_loc_after_filling_it(self) -> bool:
        driver = self.db_driver(self.db_engine)
        loader = driver.loader()
        if loader is None:
            logger.warning(f"No loader configured for this database type ({self.db_engine.name})")
            return False
        if self.can_move_from_fileobjs_source():
            # Regardless we can certainly stream the data from wherever
            # the source writes it
            return True

        has_scratch_location = loader.has_temporary_loadable_directory_loc()
        if not has_scratch_location:
            logger.warning("Loader does not have a temporary loadable "
                           f"directory location ({self.db_engine.name})")
        return has_scratch_location

    def temporary_loadable_directory_scheme(self) -> str:
        driver = self.db_driver(self.db_engine)
        loader = driver.loader()
        if loader is None:
            raise TypeError("Please check can_move_from_temp_loc_after_filling_it() "
                            "before calling this")
        return loader.temporary_loadable_directory_scheme()

    def move_from_temp_loc_after_filling_it(self,
                                            records_source:
                                            SupportsMoveToRecordsDirectory,
                                            processing_instructions: ProcessingInstructions)\
            -> MoveResult:
        return DoMoveFromTempLocAfterFillingIt(self.prep,
                                               self,
                                               self,
                                               records_source,
                                               processing_instructions).move()

    def move_from_dataframes_source(self,
                                    dfs_source: 'DataframesRecordsSource',
                                    processing_instructions:
                                    ProcessingInstructions) -> MoveResult:
        from records_mover.records.targets.table.move_from_dataframes_source import (
            DoMoveFromDataframesSource
        )

        return DoMoveFromDataframesSource(self.prep,
                                          self,
                                          self,
                                          dfs_source,
                                          processing_instructions).move()

    def __str__(self) -> str:
        return f"{type(self).__name__}({self.db_engine.name})"
