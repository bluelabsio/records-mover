from ..results import MoveResult
from ..records_directory import RecordsDirectory
from .base import (SupportsMoveFromDataframes,
                   SupportsMoveFromTempLocAfterFillingIt,
                   SupportsMoveToRecordsDirectory,
                   SupportsMoveFromRecordsDirectory)
from ..processing_instructions import ProcessingInstructions
from ...url.base import BaseFileUrl
from ..records_format import BaseRecordsFormat, DelimitedRecordsFormat
from ..compression import sniff_compression_from_url
from .fileobj import FileobjTarget
from typing import Optional, List, TYPE_CHECKING
if TYPE_CHECKING:
    from pandas import DataFrame  # noqa
    from ..sources.dataframes import DataframesRecordsSource


class DataUrlTarget(SupportsMoveFromDataframes,
                    SupportsMoveFromTempLocAfterFillingIt,
                    SupportsMoveFromRecordsDirectory):
    def __init__(self,
                 output_loc: BaseFileUrl,
                 records_format: Optional[BaseRecordsFormat]) -> None:
        if records_format is None:
            # if we have been given free rein on format, don't
            # surprise the user by writing a compression they're not
            # expecting.
            compression = sniff_compression_from_url(output_loc.url)
            inferred_hints = {
                'compression': compression
            }
            records_format = DelimitedRecordsFormat().alter_hints(inferred_hints)

        self.records_format = records_format
        self.output_loc = output_loc

    def move_from_dataframes_source(self,
                                    dfs_source: 'DataframesRecordsSource',
                                    processing_instructions:
                                    ProcessingInstructions) -> MoveResult:
        with self.output_loc.open(mode='wb') as fileobj:
            fileobj_target = FileobjTarget(fileobj=fileobj, records_format=self.records_format)
            return fileobj_target.\
                move_from_dataframes_source(dfs_source=dfs_source,
                                            processing_instructions=processing_instructions)

    def can_load_direct(self, scheme: str) -> bool:
        return scheme == self.output_loc.scheme

    def move_from_records_directory(self,
                                    directory: RecordsDirectory,
                                    processing_instructions: ProcessingInstructions,
                                    override_records_format: Optional[BaseRecordsFormat] = None)\
            -> MoveResult:
        records_format = directory.load_format(processing_instructions.fail_if_dont_understand)
        directory.save_to_url(self.output_loc)
        return MoveResult(move_count=None,
                          output_urls={
                              records_format.generate_filename('data'): self.output_loc.url
                          })

    def move_from_temp_loc_after_filling_it(self,
                                            records_source:
                                            SupportsMoveToRecordsDirectory,
                                            processing_instructions:
                                            ProcessingInstructions) -> MoveResult:
        pis = processing_instructions
        records_format = records_source.compatible_format(self)
        if records_format is None:
            raise NotImplementedError("No compatible records format between "
                                      f"{records_source} and {self}")
        with self.output_loc.temporary_directory() as temp_loc:
            directory = RecordsDirectory(records_loc=temp_loc)
            records_source.\
                move_to_records_directory(directory,
                                          records_format=records_format,
                                          processing_instructions=pis)
            return self.move_from_records_directory(directory,
                                                    processing_instructions)

    def can_move_from_this_format(self,
                                  source_records_format: BaseRecordsFormat) -> bool:
        if self.records_format is None:
            return True
        else:
            return self.records_format == source_records_format

    def known_supported_records_formats(self) -> List[BaseRecordsFormat]:
        if self.records_format is not None:
            return [self.records_format]
        return []
