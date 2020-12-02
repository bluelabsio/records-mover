from ..results import MoveResult
from ..records_directory import RecordsDirectory
from .base import (SupportsMoveFromDataframes,
                   SupportsMoveFromRecordsDirectory)
from ..processing_instructions import ProcessingInstructions
from ...url.base import BaseFileUrl
from ..records_format import (
    BaseRecordsFormat, DelimitedRecordsFormat, ParquetRecordsFormat, AvroRecordsFormat
)
from ..delimited import sniff_compression_from_url
from .fileobj import FileobjTarget
from typing import Optional, List, TYPE_CHECKING
if TYPE_CHECKING:
    from pandas import DataFrame  # noqa
    from ..sources.dataframes import DataframesRecordsSource


def sniff_records_format_from_url(url: str) -> BaseRecordsFormat:
    if url.endswith('.parquet'):
        return ParquetRecordsFormat()
    elif url.endswith('.avro'):
        return AvroRecordsFormat()
    else:
        return DelimitedRecordsFormat()


class DataUrlTarget(SupportsMoveFromDataframes,
                    SupportsMoveFromRecordsDirectory):
    def __init__(self,
                 output_loc: BaseFileUrl,
                 records_format: Optional[BaseRecordsFormat]) -> None:
        if records_format is None:
            records_format = sniff_records_format_from_url(output_loc.url)

        if (isinstance(records_format, DelimitedRecordsFormat) and
           'compression' not in records_format.custom_hints):
            # if user hasn't very specifically specified compression
            # type, don't surprise the user by writing a compression
            # they're not expecting.
            compression = sniff_compression_from_url(output_loc.url)
            inferred_hints = {
                'compression': compression
            }
            records_format = records_format.alter_hints(inferred_hints)

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

    def can_move_directly_from_scheme(self, scheme: str) -> bool:
        # Currently all means of copying between different schemes
        # involve streaming data down to Records Mover from the source
        # and then back up to the target.
        return False

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

    def can_move_from_format(self,
                             source_records_format: BaseRecordsFormat) -> bool:
        if self.records_format is None:
            return True
        else:
            return self.records_format == source_records_format

    def known_supported_records_formats(self) -> List[BaseRecordsFormat]:
        if self.records_format is not None:
            return [self.records_format]
        return []
