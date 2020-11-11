from contextlib import contextmanager
from records_mover.records.prep import TablePrep, TargetTableDetails
from records_mover.records.records_directory import RecordsDirectory
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.results import MoveResult
from records_mover.url.base import BaseDirectoryUrl
from records_mover.records.sources import SupportsMoveToRecordsDirectory
from records_mover.records.sources.table import TableRecordsSource
from records_mover.records.targets.table.base import BaseTableMoveAlgorithm
import logging
from typing import Iterator, Tuple, TYPE_CHECKING
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

    def negotiate_temporary_locations(self,
                                      temp_loadable_loc: BaseDirectoryUrl,
                                      temp_unloadable_loc: BaseDirectoryUrl) ->\
            Tuple[BaseDirectoryUrl, BaseDirectoryUrl]:
        raise NotImplementedError

    @contextmanager
    def temporary_loadable_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        driver = self.tbl.db_driver(self.tbl.db_engine)
        loader = driver.loader()
        # This will only be reached in move() if
        # Source#has_compatible_format(records_target) returns true,
        # which means we were able to get a loader and call
        # can_load_this_format() previously.
        assert loader is not None
        with loader.temporary_loadable_directory_loc() as loc:
            yield loc

    @contextmanager
    def temporary_directory_locs(self) -> Iterator[Tuple[BaseDirectoryUrl, BaseDirectoryUrl]]:
        if isinstance(self.records_source, TableRecordsSource):
            with self.records_source.temporary_unloadable_directory_loc() as temp_unloadable_loc,\
                    self.temporary_loadable_directory_loc() as temp_loadable_loc:
                if temp_unloadable_loc.scheme == temp_loadable_loc:
                    # use the same location
                    yield (temp_loadable_loc, temp_loadable_loc)
                else:
                    # use different locations
                    yield (temp_unloadable_loc, temp_loadable_loc)
        else:
            with self.temporary_loadable_directory_loc() as temp_loadable_loc:
                yield (temp_loadable_loc, temp_loadable_loc)

    def move(self) -> MoveResult:
        pis = self.processing_instructions
        records_format = self.records_source.compatible_format(self.table_target)
        if records_format is None:
            raise NotImplementedError("No compatible records format between "
                                      f"{self.records_source} and {self}")
        with self.temporary_directory_locs() as (temp_unloadable_loc, temp_loadable_loc):
            unload_directory = RecordsDirectory(records_loc=temp_unloadable_loc)
            self.records_source.\
                move_to_records_directory(records_directory=unload_directory,
                                          records_format=records_format,
                                          processing_instructions=pis)
            load_directory = RecordsDirectory(records_loc=temp_loadable_loc)
            if temp_unloadable_loc.url != temp_loadable_loc.url:
                load_directory.copy_from(temp_unloadable_loc)
            out = self.table_target.\
                move_from_records_directory(directory=load_directory,
                                            processing_instructions=self.processing_instructions)
            return out
