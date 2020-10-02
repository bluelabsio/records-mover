import itertools
from .base import SupportsToFileobjsSource
from ..processing_instructions import ProcessingInstructions
from ..pandas import pandas_to_csv_options
from ..schema import RecordsSchema
from contextlib import contextmanager
from ..records_format import BaseRecordsFormat, DelimitedRecordsFormat, ParquetRecordsFormat
from .fileobjs import FileobjsSource  # noqa
from tempfile import NamedTemporaryFile
from ..delimited import complain_on_unhandled_hints
import logging
from typing import Iterator, Iterable, Optional, Union, Dict, IO, Callable, TYPE_CHECKING
from records_mover.pandas import purge_unnamed_unused_columns
from records_mover.records.pandas import prep_df_for_csv_output
if TYPE_CHECKING:
    from pandas import DataFrame


logger = logging.getLogger(__name__)


class DataframesRecordsSource(SupportsToFileobjsSource):
    def __init__(self,
                 dfs: Iterable['DataFrame'],
                 processing_instructions: ProcessingInstructions=ProcessingInstructions(),
                 records_schema: Optional[RecordsSchema]=None,
                 include_index: bool=False) -> None:
        self.dfs = dfs
        self.processing_instructions = processing_instructions
        self.records_schema = records_schema
        self.include_index = include_index

    def pick_best_records_format(self,
                                 records_format_if_possible: Optional[BaseRecordsFormat]) ->\
            Union[ParquetRecordsFormat, DelimitedRecordsFormat]:
        # Ensure we don't accept a records format that we don't yet
        # have code to export to.
        if records_format_if_possible is None:
            return DelimitedRecordsFormat()
        if isinstance(records_format_if_possible, DelimitedRecordsFormat):
            return records_format_if_possible
        if isinstance(records_format_if_possible, ParquetRecordsFormat):
            return records_format_if_possible

        return DelimitedRecordsFormat()

    def peek(self) -> 'DataFrame':
        # https://stackoverflow.com/a/2425347/9795956
        dfs_iter = iter(self.dfs)
        first_df = next(dfs_iter)
        self.dfs = itertools.chain([first_df], dfs_iter)
        return first_df

    def initial_records_schema(self,
                               processing_instructions: ProcessingInstructions) -> RecordsSchema:
        records_schema = self.records_schema
        if records_schema is None:
            # We don't have any schema to begin with - let's
            # start with one based on this df:
            records_schema = self.schema_from_df(self.peek(), processing_instructions)
        return records_schema

    def serialize_dfs(self,
                      processing_instructions: ProcessingInstructions,
                      records_schema: RecordsSchema,
                      records_format: BaseRecordsFormat,
                      save_df: Callable[['DataFrame', str], None])\
            -> Iterator[FileobjsSource]:

        target_names_to_input_fileobjs: Dict[str, IO[bytes]] = {}
        i = 1

        for df in self.dfs:
            with NamedTemporaryFile(prefix='mover_seralized_dataframe') as output_file:
                df = purge_unnamed_unused_columns(df)
                output_filename = output_file.name
                logger.info(f"Writing CSV file to {output_filename}")
                save_df(df, output_filename)
                short_filename = records_format.generate_filename('data{:0>3}'.format(i))
                target_names_to_input_fileobjs[short_filename] = open(output_filename, 'rb')
                # pad with leading zeros to three digits so these files sort when listed
                if i == 2 and self.records_schema is None:
                    # Records mover is not yet smart enough to
                    # coalease multiple records schemas from different
                    # chunks into a single schema, so the result of
                    # initial_records_schema was based on only the
                    # first chunk.
                    #
                    # https://github.com/bluelabsio/records-mover/issues/93
                    logger.warning("Only checking first chunk for type inference")
                i = i + 1

        try:
            yield FileobjsSource(target_names_to_input_fileobjs=target_names_to_input_fileobjs,
                                 records_schema=records_schema,
                                 records_format=records_format)
        finally:
            for filename, fileobj in target_names_to_input_fileobjs.items():
                if not fileobj.closed:
                    fileobj.close()

    def schema_from_df(self, df: 'DataFrame',
                       processing_instructions: ProcessingInstructions) -> RecordsSchema:
        records_schema = RecordsSchema.from_dataframe(df,
                                                      self.processing_instructions,
                                                      include_index=self.include_index)
        if (processing_instructions.max_inference_rows is None or
           processing_instructions.max_inference_rows > 0):
            #
            # If we were provided with a RecordsSchema, assume
            # that the user wants us to use that verbatim (or they
            # can call .refine_from_dataframe() themselves.
            # Otherwise, gather information to create an efficient
            # schema on the target of the move.
            #
            records_schema = records_schema.refine_from_dataframe(df, processing_instructions)

        return records_schema

    @contextmanager
    def to_fileobjs_source(self,
                           processing_instructions: ProcessingInstructions,
                           records_format_if_possible: Optional[BaseRecordsFormat]=
                           None) -> Iterator[FileobjsSource]:
        records_format = self.pick_best_records_format(records_format_if_possible)
        records_schema = self.initial_records_schema(processing_instructions)
        if isinstance(records_format, DelimitedRecordsFormat):
            unhandled_hints = set(records_format.hints.keys())
            options = pandas_to_csv_options(records_format,
                                            unhandled_hints,
                                            self.processing_instructions)
            logger.info(f"Exporting to CSV with these Pandas options: {options}")
            complain_on_unhandled_hints(self.processing_instructions.fail_if_dont_understand,
                                        unhandled_hints, records_format.hints)

            # Convince mypy that this type will stay the same
            delimited_records_format = records_format

            def save_df(df: 'DataFrame', output_filename: str) -> None:
                df = prep_df_for_csv_output(df,
                                            include_index=self.include_index,
                                            records_schema=records_schema,
                                            records_format=delimited_records_format,
                                            processing_instructions=processing_instructions)
                df.to_csv(path_or_buf=output_filename,
                          index=self.include_index,
                          **options)
                logger.info('CSV file written')
        elif isinstance(records_format, ParquetRecordsFormat):
            # Pyarrow is the only engine we've tested with, and it
            # needed special options, so let's tell Pandas to use it
            pyarrow_args = {
                'coerce_timestamps': None
            }

            def save_df(df: 'DataFrame', output_filename: str) -> None:
                logger.info(f"Writing Parquet file to {output_filename}")
                # Note that this doesn't specify partitioning as of yet -
                # https://github.com/bluelabsio/records-mover/issues/94
                df.to_parquet(fname=output_filename,
                              engine='pyarrow',
                              index=self.include_index,
                              **pyarrow_args)
                logger.info('Parquet file written')
        else:
            raise NotImplementedError(f"Teach me how to write to {records_format}")

        return self.serialize_dfs(processing_instructions, records_schema, records_format, save_df)
