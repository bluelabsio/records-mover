import io
from shutil import copyfileobj
from tempfile import NamedTemporaryFile
from .base import SupportsMoveFromDataframes
from ..results import MoveResult
from ..processing_instructions import ProcessingInstructions
from ..records_format import BaseRecordsFormat, DelimitedRecordsFormat
from ..delimited import complain_on_unhandled_hints
import logging
from typing import IO, Union, TYPE_CHECKING
if TYPE_CHECKING:
    from pandas import DataFrame  # noqa
    from ..sources.dataframes import DataframesRecordsSource

logger = logging.getLogger(__name__)


class FileobjTarget(SupportsMoveFromDataframes):
    def __init__(self,
                 fileobj: IO[bytes],
                 records_format: BaseRecordsFormat) -> None:
        self.fileobj = fileobj
        self.records_format = records_format

    def move_from_dataframes_source(self,
                                    dfs_source: 'DataframesRecordsSource',
                                    processing_instructions:
                                    ProcessingInstructions) -> MoveResult:
        from ..pandas import pandas_to_csv_options
        from records_mover.records.pandas import prep_df_for_csv_output

        if not isinstance(self.records_format, DelimitedRecordsFormat):
            raise NotImplementedError("Teach me to export from dataframe to "
                                      f"{self.records_format.format_type}")
        unhandled_hints = set(self.records_format.hints.keys())
        options = pandas_to_csv_options(self.records_format,
                                        unhandled_hints,
                                        processing_instructions)
        complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                    unhandled_hints, self.records_format.hints)
        logger.info(f"Writing CSV file to {self.fileobj} with options {options}...")
        encoding: str = self.records_format.hints['encoding']  # type: ignore

        records_schema = dfs_source.initial_records_schema(processing_instructions)
        records_format = self.records_format

        def write_dfs(path_or_buf: Union[str, IO[str]]) -> int:
            first_row = True
            move_count = 0
            for df in dfs_source.dfs:
                logger.info("Appending from dataframe...")

                # Include the header at most once in the file
                include_header_row = options['header'] and first_row
                first_row = False
                options['header'] = include_header_row
                df = prep_df_for_csv_output(df,
                                            include_index=dfs_source.include_index,
                                            records_schema=records_schema,
                                            records_format=records_format,
                                            processing_instructions=processing_instructions)
                df.to_csv(path_or_buf=path_or_buf,
                          mode="a",
                          index=dfs_source.include_index,
                          **options)
                move_count += len(df.index)
            return move_count

        # Pandas' df.to_csv won't write a compressed file to a stream
        # (bleh).  Instead, if compression is set, write to a temp
        # file first.
        if self.records_format.hints['compression'] is None:
            text_fileobj = io.TextIOWrapper(self.fileobj, encoding=encoding)
            move_count = write_dfs(text_fileobj)
            text_fileobj.detach()
        else:
            with NamedTemporaryFile(prefix='mover_fileobj_target') as output_file:
                move_count = write_dfs(output_file.name)
                with open(output_file.name, "rb") as output_fileobj:
                    copyfileobj(output_fileobj, self.fileobj)

        logger.info('CSV file written')
        return MoveResult(output_urls=None, move_count=move_count)
