from records_mover.db import DBDriver
from records_mover.records.prep import TablePrep, TargetTableDetails
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.results import MoveResult
from records_mover.records.prep_and_load import prep_and_load
from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.records.sources.fileobjs import FileobjsSource
from records_mover.records.targets.table.base import BaseTableMoveAlgorithm
from records_mover.utils.concat_files import ConcatFiles
from typing import Optional, IO
import logging

logger = logging.getLogger(__name__)


class DoMoveFromFileobjsSource(BaseTableMoveAlgorithm):
    fileobj: IO[bytes]

    def __init__(self,
                 prep: TablePrep,
                 target_table_details: TargetTableDetails,
                 fileobjs_source: FileobjsSource,
                 processing_instructions: ProcessingInstructions) -> None:
        self.fileobjs_source = fileobjs_source
        all_fileobjs = list(self.fileobjs_source.target_names_to_input_fileobjs.values())
        if len(all_fileobjs) != 1:
            self.fileobj = ConcatFiles(all_fileobjs)  # type: ignore
        else:
            self.fileobj = all_fileobjs[0]
        self.records_format = self.fileobjs_source.records_format
        self.plan = RecordsLoadPlan(records_format=self.records_format,
                                    processing_instructions=processing_instructions)
        super().__init__(prep, target_table_details, processing_instructions)

    def load(self, driver: DBDriver) -> Optional[int]:
        loader_from_fileobj = driver.loader_from_fileobj()
        # This is only reached in move() when
        # records_target.can_move_from_fileobjs_source() is true,
        # which is only true when .load_from_fileobj() is not None.
        assert loader_from_fileobj is not None
        return loader_from_fileobj.load_from_fileobj(schema=self.tbl.schema_name,
                                                     table=self.tbl.table_name,
                                                     load_plan=self.plan,
                                                     fileobj=self.fileobj)

    def reset_before_reload(self) -> None:
        if not self.tbl.drop_and_recreate_on_load_error:
            raise
        if not self.fileobj.seekable():
            raise NotImplementedError("This process cannot be restarted with this "
                                      "type of file object. Please fix your table "
                                      "manually and try the load again.")
        self.fileobj.seek(0)

    def move(self) -> MoveResult:
        with self.tbl.db_engine.begin() as db:
            driver = self.tbl.db_driver(db)
            schema_obj = self.fileobjs_source.records_schema
            schema_sql = self.schema_sql_for_load(schema_obj, self.records_format, driver)
            loader_from_fileobj = driver.loader_from_fileobj()
            # This is only reached in move() when
            # records_target.can_move_from_fileobjs_source() is true,
            # which is only true when .load_from_fileobj() is not None.
            assert loader_from_fileobj is not None
            load_exception = loader_from_fileobj.load_failure_exception()

        return prep_and_load(self.tbl, self.prep, schema_sql, self.load,
                             load_exception,
                             self.reset_before_reload)
