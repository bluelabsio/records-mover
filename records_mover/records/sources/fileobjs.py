from tempfile import TemporaryDirectory
from contextlib import contextmanager
from .base import (SupportsMoveToRecordsDirectory,
                   SupportsToDataframesSource)
from ..records_directory import RecordsDirectory
from ...utils.concat_files import ConcatFiles
import io
from ..results import MoveResult
from ..records_format import BaseRecordsFormat, DelimitedRecordsFormat
from ..delimited import sniff_hints_from_fileobjs
from .. import PartialRecordsHints
from ..processing_instructions import ProcessingInstructions
from ...records.delimited import complain_on_unhandled_hints
from ..delimited import python_encoding_from_hint
from ..schema import RecordsSchema
from records_mover.url.filesystem import FilesystemDirectoryUrl
from records_mover.url.base import BaseDirectoryUrl
import logging
from typing import Mapping, IO, Optional, Iterator, List, Any, TYPE_CHECKING
if TYPE_CHECKING:
    from .dataframes import DataframesRecordsSource  # noqa


logger = logging.getLogger(__name__)


class FileobjsSource(SupportsMoveToRecordsDirectory,
                     SupportsToDataframesSource):
    def __init__(self,
                 target_names_to_input_fileobjs: Mapping[str, IO[bytes]],
                 records_format: BaseRecordsFormat,
                 records_schema: RecordsSchema) -> None:
        self.target_names_to_input_fileobjs = target_names_to_input_fileobjs
        self.records_format = records_format
        self.records_schema = records_schema

    def known_supported_records_formats(self) -> List[BaseRecordsFormat]:
        return [self.records_format]

    def can_move_to_format(self,
                           target_records_format: BaseRecordsFormat) -> bool:
        return self.records_format == target_records_format

    def can_move_to_scheme(self, scheme: str) -> bool:
        # Any URL can accept a stream of data using
        # move_to_records_directory() below
        return True

    @staticmethod
    @contextmanager
    def infer_if_needed(target_names_to_input_fileobjs: Mapping[str, IO[bytes]],
                        processing_instructions: ProcessingInstructions,
                        records_format: Optional[BaseRecordsFormat],
                        records_schema: Optional[RecordsSchema],
                        initial_hints: Optional[PartialRecordsHints]) ->\
            Iterator['FileobjsSource']:
        try:
            if records_format is None:
                if initial_hints is None:
                    initial_hints = {}
                logger.info(f"Determining records format with initial_hints={initial_hints}")
                inferred_hints =\
                    sniff_hints_from_fileobjs(list(target_names_to_input_fileobjs.values()),
                                              initial_hints=initial_hints)
                # 'csv' isn't the most precise variant or fastest
                # variant to read, but given it's the default for Excel
                # and Google Sheets, it's the most common on import.  So,
                # if the specific variant isn't specified by the user,
                # let's assume 'csv'.
                records_format = DelimitedRecordsFormat(variant='csv',
                                                        hints=inferred_hints)
            if records_schema is None:
                records_schema =\
                    RecordsSchema.from_fileobjs(list(target_names_to_input_fileobjs.values()),
                                                records_format=records_format,
                                                processing_instructions=processing_instructions)

            yield FileobjsSource(target_names_to_input_fileobjs=target_names_to_input_fileobjs,
                                 records_format=records_format,
                                 records_schema=records_schema)
        except UnicodeDecodeError:
            if isinstance(records_format, DelimitedRecordsFormat):
                hints = records_format.hints if records_format else {'encoding': 'UTF8'}
                encoding = hints['encoding']
                raise TypeError("Please try again using a different encoding--"
                                f"{encoding} did not work")
            elif records_format is None:
                raise TypeError("Please try again and specify a particular records format--"
                                "assuming delimited with UTF-8 encoding did not work")
            else:
                raise

    def move_to_records_directory(self,
                                  records_directory: RecordsDirectory,
                                  records_format: BaseRecordsFormat,
                                  processing_instructions: ProcessingInstructions) -> MoveResult:

        def filename_from_url(url: str) -> str:
            return url[url.rfind('/')+1:]

        if records_format != self.records_format:
            raise NotImplementedError(f"This directory can only accept {self.records_format}")
        url_details = records_directory.save_fileobjs(self.target_names_to_input_fileobjs,
                                                      records_schema=self.records_schema,
                                                      records_format=self.records_format)
        output_urls = {
            filename_from_url(url): url
            for url in url_details
        }
        return MoveResult(output_urls=output_urls, move_count=None)

    @contextmanager
    def temporary_unloadable_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        """Yield a temporary directory that can be used to call move_to_records_directory() on."""
        with TemporaryDirectory(prefix='temporary_unloadable_directory_loc') as dirname:
            yield FilesystemDirectoryUrl(dirname)

    @contextmanager
    def to_dataframes_source(self,
                             processing_instructions: ProcessingInstructions) \
            -> Iterator['DataframesRecordsSource']:
        import pandas as pd
        from .dataframes import DataframesRecordsSource  # noqa
        from ..pandas import pandas_read_csv_options

        """Convert current source to a DataframeSource and present it in a context manager"""
        if not isinstance(self.records_format, DelimitedRecordsFormat):
            raise NotImplementedError("Teach me to convert from "
                                      f"{self.records_format.format_type} to dataframe")
        unhandled_hints = set(self.records_format.hints.keys())
        options = pandas_read_csv_options(self.records_format,
                                          self.records_schema,
                                          unhandled_hints,
                                          processing_instructions)
        complain_on_unhandled_hints(processing_instructions.fail_if_dont_understand,
                                    unhandled_hints,
                                    self.records_format.hints)
        logger.info(f"Loading CSV via Pandas with options: {options}")
        hints = self.records_format.hints
        fileobjs = list(self.target_names_to_input_fileobjs.values())
        single_fileobj: IO[bytes] = ConcatFiles(fileobjs)  # type: ignore
        target_fileobj: IO[Any] = single_fileobj
        text_fileobj = None
        if hints['compression'] is None:
            hint_encoding: str = hints['encoding']  # type: ignore
            python_encoding = python_encoding_from_hint[hint_encoding]  # type: ignore
            text_fileobj = io.TextIOWrapper(single_fileobj, encoding=python_encoding)
            target_fileobj = text_fileobj
        try:
            num_fields = len(self.records_schema.fields)
            entries_per_chunk = 2000000
            if num_fields == 0:
                chunksize = entries_per_chunk
            else:
                chunksize = int(entries_per_chunk / num_fields)

            try:
                dfs = pd.read_csv(filepath_or_buffer=target_fileobj,
                                  iterator=True,
                                  chunksize=chunksize,
                                  **options)
            except pd.errors.EmptyDataError:
                dfs = [self.records_schema.to_empty_dataframe()]
            yield DataframesRecordsSource(dfs=dfs, records_schema=self.records_schema)
        finally:
            if text_fileobj is not None:
                text_fileobj.detach()

    def __str__(self) -> str:
        return f"{type(self).__name__}({self.records_format})"
