from abc import ABCMeta, abstractmethod
from ..records_directory import RecordsDirectory
from ..records_format import BaseRecordsFormat
from ..processing_instructions import ProcessingInstructions
from ..sources import SupportsMoveToRecordsDirectory
from ..results import MoveResult
from typing import Optional, List, TYPE_CHECKING
from ..sources.fileobjs import FileobjsSource
if TYPE_CHECKING:
    from pandas import DataFrame  # noqa
    from ..sources.dataframes import DataframesRecordsSource


class RecordsTarget(metaclass=ABCMeta):
    """These are possible abstract base classes you can implement --
    implement all which can be done efficiently for your
    RecordsTarget.  See move() in mover.py to see how these are used
    or to debug a particular records move scenario).  None are
    required, but a more fully featured implementation is more likely
    to work with the current move() algorithms and allow moving
    records efficiently from a wider range of RecordsSource
    classes."""

    def validate(self) -> None:
        """Raise exception if this target as configured is not valid in some
        way (e.g., directory not empty, DB creds don't work, etc)"""
        pass

    def __str__(self) -> str:
        return type(self).__name__


class NegotiatesRecordsFormat(RecordsTarget, metaclass=ABCMeta):
    @abstractmethod
    def known_supported_records_formats(self) -> List[BaseRecordsFormat]:
        pass

    @abstractmethod
    def can_move_from_format(self,
                             source_records_format: BaseRecordsFormat) -> bool:
        """Returns True if any movement operations can be done using the specified format
        without translation beforehand to a different format"""
        pass


class SupportsRecordsDirectory(NegotiatesRecordsFormat, metaclass=ABCMeta):
    # If a certain format is required when writing to this target, set it here
    records_format: Optional[BaseRecordsFormat]

    def known_supported_records_formats(self) -> List[BaseRecordsFormat]:
        if self.records_format is not None:
            return [self.records_format]
        else:
            return []

    def can_move_from_format(self,
                             source_records_format: BaseRecordsFormat) -> bool:
        """Returns True if any movement operations can be done using the specified format
        without translation beforehand to a different format"""
        return self.records_format is None or self.records_format == source_records_format

    @abstractmethod
    def records_directory(self) -> RecordsDirectory:
        """Represent the current target as a RecordsDirectory object which
        can be filled in.  Note that this is not a context manager, so
        don't implement this method by creating things which need
        cleanup (e.g., creating a temporary directory).  Please see
        to_fileobjs_source() for that situation."""
        pass

    def pre_load_hook(self) -> None:
        """This function will be called before data is loaded into the records
        directory.

        You can expect any exceptions raised by your implementation of
        this hook to be propagated back to the original caller of
        move() - there's no guarantee they will be handled at any
        layer of the move() implementation."""
        pass

    def post_load_hook(self, num_rows_loaded: Optional[int]) -> None:
        """This function will be called after data is loaded into the records
        directory

        You can expect any exceptions raised by your implementation of
        this hook to be propagated back to the original caller of
        move() - there's no guarantee they will be handled at any
        layer of the move() implementation."""
        pass


class SupportsMoveFromRecordsDirectory(NegotiatesRecordsFormat, metaclass=ABCMeta):
    @abstractmethod
    def move_from_records_directory(self,
                                    directory: RecordsDirectory,
                                    processing_instructions: ProcessingInstructions,
                                    override_records_format: Optional[BaseRecordsFormat]=None)\
            -> MoveResult:

        """Given a RecordsDirectory object, load the data inside
        per the ProcessingInstructions and any hint overrides provided."""
        pass

    @abstractmethod
    def can_move_directly_from_scheme(self, scheme: str) -> bool:
        """If true is returned, the load will be done without streaming data
        down to Records Mover byte by byte--which can be expensive
        when data is large and/or network bandwidth is limited.  A
        target that can read from a scheme "directly" in this sense is
        more likely to be efficient in loading."""
        pass


class MightSupportMoveFromFileobjsSource(NegotiatesRecordsFormat, metaclass=ABCMeta):
    @abstractmethod
    def move_from_fileobjs_source(self,
                                  fileobjs_source: FileobjsSource,
                                  processing_instructions: ProcessingInstructions) -> MoveResult:
        pass

    @abstractmethod
    def can_move_from_fileobjs_source(self) -> bool:
        pass


class MightSupportMoveFromTempLocAfterFillingIt(NegotiatesRecordsFormat, metaclass=ABCMeta):
    @abstractmethod
    def can_move_from_temp_loc_after_filling_it(self) -> bool:
        """Returns True if target as currently configured can be handed a
        temporary location and fill it.
        """
        pass

    @abstractmethod
    def temporary_loadable_directory_scheme(self) -> str:
        """Which URL scheme will be used to create the temporary location to fill in."""
        pass

    @abstractmethod
    def move_from_temp_loc_after_filling_it(self,
                                            records_source:
                                            SupportsMoveToRecordsDirectory,
                                            processing_instructions:
                                            ProcessingInstructions) -> MoveResult:

        """Create a temporary location for a RecordsDirectory to live,
        call records_source.move_to_records_directory()
        with it, and then move in the records from the temporary
        RecordsDirectory location.  This can be useful when copying
        records from sources and targets where neither represent
        themselves as a RecordsDirectory (e.g., pandas dataframe
        to/from database), but where the target can load from one if
        it's available.  Not needed if you can implement
        records_directory()."""
        pass


class SupportsMoveFromDataframes(RecordsTarget, metaclass=ABCMeta):
    @abstractmethod
    def move_from_dataframes_source(self,
                                    dfs_source: 'DataframesRecordsSource',
                                    processing_instructions:
                                    ProcessingInstructions) -> MoveResult:
        """
        Given DataFramesRecordsSource, load the data inside per the
        ProcessingInstructions provided.
        """
        pass
