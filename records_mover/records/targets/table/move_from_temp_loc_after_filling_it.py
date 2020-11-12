from contextlib import contextmanager
from records_mover.records.prep import TablePrep, TargetTableDetails
from records_mover.records.records_directory import RecordsDirectory
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover.records.results import MoveResult
from records_mover.url.base import BaseDirectoryUrl
from records_mover.url.gcs.gcs_directory_url import GCSDirectoryUrl
from records_mover.url.s3.s3_directory_url import S3DirectoryUrl
from records_mover.records.sources import SupportsMoveToRecordsDirectory
from records_mover.records.sources.table import TableRecordsSource
from records_mover.records.targets.table.base import BaseTableMoveAlgorithm
import logging
from typing import Iterator, Tuple, Union, Optional, TYPE_CHECKING
if TYPE_CHECKING:
    from .target import TableRecordsTarget  # Dodge circular dependency

logger = logging.getLogger(__name__)


class CopyOptimizer:
    # TODO: move to new location
    # TODO: Bring in Google code
    # TODO: Figure out where this should be called from

    def try_swapping_bucket_path(self,
                                 target_bucket: Union[GCSDirectoryUrl, S3DirectoryUrl],
                                 bucket_to_steal_path_from: Union[GCSDirectoryUrl,
                                                                  S3DirectoryUrl]) ->\
                Optional[Union[GCSDirectoryUrl, S3DirectoryUrl]]:
        optimized_directory =\
            target_bucket.directory_in_this_bucket(bucket_to_steal_path_from.key)
        if not optimized_directory.empty():
            logger.info(f"{optimized_directory} is not empty")
            return None

        if not optimized_directory.writable():
            logger.info(f"{optimized_directory} is not writable")
            return None

        return optimized_directory

    @contextmanager
    def optimize_temp_locations_for_gcp_data_transfer(self,
                                                      temp_unloadable_loc: S3DirectoryUrl,
                                                      temp_loadable_loc: GCSDirectoryUrl) ->\
            Iterator[Tuple[BaseDirectoryUrl, BaseDirectoryUrl]]:
        #
        # GCP data transfer is great, but has the limitations:
        #
        # 1) It only works from GCS -> GCS or S3 -> GCS - like the
        # roach motel, you can check in but you can't check out.
        #
        # 2) You can't specify a different destination location
        # directory in the GCS bucket than in your source bucket
        #
        # So let's make sure that if at all possible, we use the same
        # directory.
        optimized_temp_unloadable_loc = self.try_swapping_bucket_path(temp_unloadable_loc,
                                                                      temp_loadable_loc)
        if optimized_temp_unloadable_loc is not None:
            yield (optimized_temp_unloadable_loc, temp_loadable_loc)
            return
        optimized_temp_loadable_loc = self.try_swapping_bucket_path(temp_loadable_loc,
                                                                    temp_unloadable_loc)
        if optimized_temp_loadable_loc is not None:
            yield (temp_unloadable_loc, optimized_temp_loadable_loc)
            return
        logger.warning("Could not match paths between source and destination buckets--"
                       "will not be able to use Google Storage Transfer Service for "
                       "cloud-based copy.")
        yield (temp_unloadable_loc, temp_loadable_loc)

    @contextmanager
    def optimize_temp_locations(self,
                                temp_unloadable_loc: BaseDirectoryUrl,
                                temp_loadable_loc: BaseDirectoryUrl) ->\
            Iterator[Tuple[BaseDirectoryUrl, BaseDirectoryUrl]]:
        if (isinstance(temp_unloadable_loc, S3DirectoryUrl) and
           isinstance(temp_loadable_loc, GCSDirectoryUrl)):
            with self.optimize_temp_locations_for_gcp_data_transfer(temp_unloadable_loc,
                                                                    temp_loadable_loc) as\
                    (optimized_unloadable_loc, optimized_loadable_loc):
                logger.info(f"Optimized bucket locations: {optimized_unloadable_loc}, "
                            f"{optimized_loadable_loc}")
                yield (optimized_unloadable_loc, optimized_loadable_loc)
        elif temp_unloadable_loc.scheme == temp_loadable_loc.scheme:
            # Let's use the same location!
            yield (temp_unloadable_loc, temp_unloadable_loc)
        else:
            #
            # No optimizations match
            #
            yield (temp_unloadable_loc, temp_loadable_loc)


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
        with loader.temporary_loadable_directory_loc() as loc:
            yield loc

    @contextmanager
    def temporary_directory_locs(self) -> Iterator[Tuple[BaseDirectoryUrl, BaseDirectoryUrl]]:
        # If we are unloading from a table and loading from a
        # table, let's make sure we pick the optimal unload
        # and load buckets (and ideally ensure they are one
        # and the same)

        #
        # Optimize where we load from and to
        #
        if isinstance(self.records_source, TableRecordsSource):
            with self.records_source.temporary_unloadable_directory_loc() as temp_unloadable_loc,\
                    self.temporary_loadable_directory_loc() as temp_loadable_loc:
                if temp_unloadable_loc.scheme == temp_loadable_loc:
                    # use the same location
                    yield (temp_loadable_loc, temp_loadable_loc)
                else:
                    copy_optimizer = CopyOptimizer()
                    with copy_optimizer.optimize_temp_locations(temp_unloadable_loc,
                                                                temp_loadable_loc) as\
                            (optimized_temp_unloadable_loc, optimized_temp_loadable_loc):
                        yield (optimized_temp_unloadable_loc, optimized_temp_loadable_loc)
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
