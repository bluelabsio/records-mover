from abc import ABCMeta, abstractmethod
from ..records_directory import RecordsDirectory
from ..results import MoveResult
from ..records_format import BaseRecordsFormat
from ..processing_instructions import ProcessingInstructions
from records_mover.url.base import BaseDirectoryUrl
from contextlib import contextmanager
from typing import Iterator, List, Tuple
import itertools
from typing import TYPE_CHECKING, Optional
import logging
if TYPE_CHECKING:
    # http://mypy.readthedocs.io/en/latest/common_issues.html#import-cycles
    from .fileobjs import FileobjsSource  # noqa
    from .dataframes import DataframesRecordsSource  # noqa
    from ..targets import base as targets_base  # noqa

logger = logging.getLogger(__name__)


class RecordsSource(metaclass=ABCMeta):
    """These are possible abstract base classes you can implement --
    implement all which can be done efficiently for your
    RecordsSource.  See move() in mover.py to see how these are used
    or to debug a particular records move scenario).  None are
    required, but a more fully featured implementation is more likely
    to work with the current move() algorithms and allow moving
    records to a wider range of RecordsTarget classes."""

    def __str__(self) -> str:
        return type(self).__name__

    def validate(self) -> None:
        """Raise exception if this source as configured is not valid in some
        way (e.g., table doesn't exist, directory contains no files, etc)"""
        pass


class NegotiatesRecordsFormat(RecordsSource, metaclass=ABCMeta):
    def compatible_format(self,
                          records_target: 'targets_base.NegotiatesRecordsFormat')\
            -> Optional[BaseRecordsFormat]:
        source_formats = self.known_supported_records_formats()
        target_formats = records_target.known_supported_records_formats()
        compatible_format = None
        ranked_candidates: List[Tuple[BaseRecordsFormat,
                                      BaseRecordsFormat]] =\
            list(itertools.zip_longest(source_formats, target_formats))
        # Look at the candidates in order of their appearance, so if
        # there's a suboptimal records format, those can be put at the
        # end of the lists returned by
        # known_supported_records_formats() and the more preferred
        # ones can be put at the beginning.
        for source_candidate, target_candidate in ranked_candidates:
            if source_candidate is not None:
                if records_target.can_move_from_format(source_candidate):
                    compatible_format = source_candidate
                    break
            if target_candidate is not None:
                if self.can_move_to_format(target_candidate):
                    compatible_format = target_candidate
                    break
        if compatible_format is None:
            logger.warning(f"Mover: {self} is known to handle {source_formats} but "
                           f"is not able to directly export to {records_target}, "
                           f"which is known to handle {target_formats}")
        return compatible_format

    def has_compatible_format(self,
                              records_target: 'targets_base.NegotiatesRecordsFormat')\
            -> bool:
        return self.compatible_format(records_target) is not None

    @abstractmethod
    def known_supported_records_formats(self) -> List[BaseRecordsFormat]:
        pass

    @abstractmethod
    def can_move_to_format(self,
                           target_records_format: BaseRecordsFormat) -> bool:
        """Return true if writing the specified format satisfies our format
        needs"""
        pass


class SupportsRecordsDirectory(RecordsSource, metaclass=ABCMeta):
    records_format: BaseRecordsFormat

    @abstractmethod
    def records_directory(self) -> RecordsDirectory:
        """Represent the current source as a RecordsDirectory object.
        Note that this is not a context manager, so don't implement
        this method by creating things which need cleanup (e.g.,
        creating a temporary directory).  Please see
        to_fileobjs_source() for that situation."""
        pass


class SupportsMoveToRecordsDirectory(NegotiatesRecordsFormat, metaclass=ABCMeta):
    @abstractmethod
    def can_move_to_scheme(self, scheme: str) -> bool:
        """If true is returned, the given scheme is a compatible place where
        the unload will be done.  Note that this may include streaming
        data down to Records Mover byte by byte--which can be
        expensive when data is large and/or network bandwidth is
        limited.
        """
        pass

    @abstractmethod
    def move_to_records_directory(self,
                                  records_directory: RecordsDirectory,
                                  records_format: BaseRecordsFormat,
                                  processing_instructions: ProcessingInstructions) -> MoveResult:
        """Given an empty RecordsDirectory object, a RecordsFormat object
        representing the goal representation, and
        ProcessingInstructions, move the records into the target
        directory while converting their format."""
        pass

    @contextmanager
    @abstractmethod
    def temporary_unloadable_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        """Yield a temporary directory that can be used to call move_to_records_directory() on."""
        pass


class SupportsToFileobjsSource(RecordsSource, metaclass=ABCMeta):
    @abstractmethod
    @contextmanager
    def to_fileobjs_source(self,
                           processing_instructions: ProcessingInstructions,
                           records_format_if_possible: Optional[BaseRecordsFormat]=None)\
            -> Iterator['FileobjsSource']:
        """Convert current source to a FileObjsSource and present it in a context manager.
        If there's no native records format, prefer 'records_format_if_possible' if provided
        and the source is capable of exporting to that format."""
        pass


class SupportsToDataframesSource(RecordsSource, metaclass=ABCMeta):
    @abstractmethod
    @contextmanager
    def to_dataframes_source(self,
                             processing_instructions: ProcessingInstructions) \
            -> Iterator['DataframesRecordsSource']:
        """Convert current source to a DataframeSource and present it in a context manager"""
        pass
