from .sources import (RecordsSource, SupportsMoveToRecordsDirectory,
                      SupportsToFileobjsSource,
                      FileobjsSource, SupportsToDataframesSource)
from .targets.base import (RecordsTarget, SupportsMoveFromRecordsDirectory,
                           SupportsMoveFromTempLocAfterFillingIt,
                           MightSupportMoveFromFileobjsSource,
                           SupportsMoveFromDataframes)
from .sources import base as sources_base
from .targets import base as targets_base
from .processing_instructions import ProcessingInstructions
from .records_format import BaseRecordsFormat
from .results import MoveResult
from typing import Union, ContextManager, TypeVar, cast
import logging

logger = logging.getLogger(__name__)


A = TypeVar('A')


def unpack_context_manager(potential_iterator: Union[A, ContextManager[A]]) -> A:
    if hasattr(potential_iterator, '__enter__'):
        iterator = cast(ContextManager[A], potential_iterator)
        with iterator as unpacked:
            return unpacked
    else:
        a = cast(A, potential_iterator)
        return a


def move(records_source: Union[RecordsSource, ContextManager[RecordsSource]],
         records_target: Union[RecordsTarget, ContextManager[RecordsTarget]],
         processing_instructions: ProcessingInstructions=ProcessingInstructions()) -> MoveResult:
    """Copy records from one location to another.  Applies a sequence of
    possible techniques to do this in an efficient way by looking for
    whether copy-related methods are implementeds on the source and
    target.

    To figure out how to get your new source or target to work well
    with move, follow this function down from the top and implement
    the first method which makes sense for your new source/target.
    See documentation for RecordsSource in sources.py and
    RecordsTarget in targets.py for the semantics of the methods being
    called.
    """
    records_source = unpack_context_manager(records_source)
    records_target = unpack_context_manager(records_target)

    records_source.validate()
    records_target.validate()

    if (isinstance(records_source, sources_base.SupportsRecordsDirectory) and
        isinstance(records_target, SupportsMoveFromRecordsDirectory) and
       records_target.can_load_direct(records_source.records_directory().loc.scheme) and
       records_target.can_move_from_this_format(records_source.records_format)):
        # Tell the destination to load directly from wherever the
        # source is, without needing to make any copies of the data or
        # streaming it through the current box.
        directory = records_source.records_directory()
        logger.info(f"Mover: copying from {records_source} to {records_target} "
                    f"by moving from records directory in {directory}...")
        return records_target.\
            move_from_records_directory(directory=directory,
                                        override_records_format=records_source.records_format,
                                        processing_instructions=processing_instructions)
    elif (isinstance(records_source, FileobjsSource) and
          isinstance(records_target, MightSupportMoveFromFileobjsSource) and
          records_target.can_move_from_fileobjs_source() and
          records_target.can_move_from_this_format(records_source.records_format)):
        logger.info(f"Mover: copying from {records_source} to {records_target} "
                    "by moving directly from stream...")
        # See if we can stream from the source to the destination directly
        return records_target.move_from_fileobjs_source(records_source,
                                                        processing_instructions)
    elif (isinstance(records_source, SupportsMoveToRecordsDirectory) and
          isinstance(records_target, targets_base.SupportsRecordsDirectory) and
          records_source.has_compatible_format(records_target)):
        # if target can accept records and doesn't specify a
        # records_format, or uses the same as the source, we can just
        # dump bytes directly!
        directory = records_target.records_directory()
        logger.info(f"Mover: copying from {records_source} to {records_target} "
                    f"by writing to {directory.scheme} records directory")
        pi = processing_instructions
        records_format = records_source.compatible_format(records_target)
        assert records_format is not None  # we checked compatibility above
        records_target.pre_load_hook()
        out = records_source.\
            move_to_records_directory(records_directory=directory,
                                      records_format=records_format,
                                      processing_instructions=pi)
        records_target.post_load_hook(num_rows_loaded=out.move_count)
        return out
    elif (isinstance(records_source, sources_base.SupportsRecordsDirectory) and
          isinstance(records_target, SupportsMoveFromRecordsDirectory) and
          records_target.can_move_from_this_format(records_source.records_format)):
        directory = records_source.records_directory()
        logger.info(f"Mover: copying from {records_source} to {records_target} "
                    "by loading from {directory.scheme} directory...")
        return records_target.\
            move_from_records_directory(directory=directory,
                                        override_records_format=records_source.records_format,
                                        processing_instructions=processing_instructions)
    elif isinstance(records_source, SupportsToFileobjsSource):
        target_records_format: BaseRecordsFormat = getattr(records_target, "records_format", None)
        logger.info(f"Mover: copying from {records_source} to {records_target} "
                    f"by first writing {records_source} to {target_records_format} "
                    "records format (if easy to rewrite)...")
        with records_source.\
                to_fileobjs_source(processing_instructions=processing_instructions,
                                   records_format_if_possible=target_records_format)\
                as fileobjs_source:
            return move(fileobjs_source, records_target, processing_instructions)
    elif (isinstance(records_source, SupportsMoveToRecordsDirectory) and
          isinstance(records_target, SupportsMoveFromTempLocAfterFillingIt) and
          records_source.has_compatible_format(records_target)):
        logger.info(f"Mover: copying from {records_source} to {records_target} "
                    f"by filling in a temporary location...")
        return records_target.move_from_temp_loc_after_filling_it(records_source,
                                                                  processing_instructions)
    elif (isinstance(records_source, SupportsToDataframesSource) and
          isinstance(records_target, SupportsMoveFromDataframes)):
        logger.info(f"Mover: copying from {records_source} to {records_target} "
                    f"by converting to dataframe...")
        with records_source.to_dataframes_source(processing_instructions) as dataframes_source:
            return records_target.\
                move_from_dataframes_source(dfs_source=dataframes_source,
                                            processing_instructions=processing_instructions)
    elif (isinstance(records_source, SupportsToDataframesSource)):
        logger.info(f"Mover: copying from {records_source} to {records_target} "
                    f"by first converting to dataframe...")
        with records_source.to_dataframes_source(processing_instructions) as dataframes_source:
            return move(dataframes_source, records_target, processing_instructions)
    else:
        raise NotImplementedError(f"Please teach me how to move records between "
                                  f"{records_source} and {records_target}")
