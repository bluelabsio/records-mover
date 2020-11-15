from contextlib import contextmanager
from records_mover.records.prep import TablePrep, TargetTableDetails
from records_mover.records.records_directory import RecordsDirectory
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.results import MoveResult
from records_mover.url.base import BaseDirectoryUrl
from records_mover.records.sources import SupportsMoveToRecordsDirectory
from records_mover.records.targets.table.base import BaseTableMoveAlgorithm
import logging
from typing import Iterator, TYPE_CHECKING
if TYPE_CHECKING:
    from .target import TableRecordsTarget  # Dodge circular dependency

logger = logging.getLogger(__name__)


class DoMoveFromTempLocAfterFillingIt(BaseTableMoveAlgorithm):
    def __init__(self,
                 prep: TablePrep,
                 target_table_details: TargetTableDetails,
                 table_target: 'TableRecordsTarget',
                 records_source: SupportsMoveToRecordsDirectory,
                 processing_instructions: ProcessingInstructions) -> None:
        self.table_target = table_target
        self.records_source = records_source
        super().__init__(prep, target_table_details, processing_instructions)

    @contextmanager
    def temporary_loadable_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        driver = self.tbl.db_driver(self.tbl.db_engine)
        loader = driver.loader()
        # This will only be reached in move() if
        # Source#has_compatible_format(records_target) returns true,
        # which means we were able to get a loader and call
        # can_load_this_format() previously.
        assert loader is not None
        if loader.has_temporary_loadable_directory_loc():
            with loader.temporary_loadable_directory_loc() as loc:
                yield loc
        else:
            # As a last resort, use the source's temporary directory
            with self.records_source.temporary_unloadable_directory_loc() as loc:
                yield loc

    def move(self) -> MoveResult:
        pis = self.processing_instructions
        records_format = self.records_source.compatible_format(self.table_target)
        if records_format is None:
            raise NotImplementedError("No compatible records format between "
                                      f"{self.records_source} and {self}")
        with self.temporary_loadable_directory_loc() as temp_loc:
            directory = RecordsDirectory(records_loc=temp_loc)
            self.records_source.\
                move_to_records_directory(records_directory=directory,
                                          records_format=records_format,
                                          processing_instructions=pis)
            out = self.table_target.\
                move_from_records_directory(directory=directory,
                                            processing_instructions=self.processing_instructions)
            return out
