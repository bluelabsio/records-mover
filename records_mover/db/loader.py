from ..records.records_format import BaseRecordsFormat, DelimitedRecordsFormat
from ..records.types import RecordsFormatType
from ..records.load_plan import RecordsLoadPlan
from ..records.records_directory import RecordsDirectory
from ..url.filesystem import FilesystemDirectoryUrl
from contextlib import contextmanager
from ..url.base import BaseDirectoryUrl
from tempfile import TemporaryDirectory
from abc import ABCMeta, abstractmethod
from typing import Optional, Type, Iterator, IO, List
import sqlalchemy


class NegotiatesLoadFormat(metaclass=ABCMeta):
    @abstractmethod
    def best_records_format(self) -> BaseRecordsFormat:
        ...

    @abstractmethod
    def load_failure_exception(self) -> Type[Exception]:
        ...

    @abstractmethod
    @contextmanager
    def temporary_loadable_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        ...

    @abstractmethod
    def known_supported_records_formats_for_load(self) -> List[BaseRecordsFormat]:
        ...

    @abstractmethod
    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        ...


class NegotiatesLoadFormatImpl(NegotiatesLoadFormat):
    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        """Return true if the specified format is compatible with the load()
        method"""
        return False

    def best_records_format(self) -> BaseRecordsFormat:
        variant = self.best_records_format_variant('delimited')
        assert variant is not None  # always provided for 'delimited'
        return DelimitedRecordsFormat(variant=variant)

    def best_records_format_variant(self,
                                    records_format_type: RecordsFormatType) -> \
            Optional[str]:
        """Return the most suitable records format delimited variant for
        loading and unloading.  This is generally the format that the
        database unloading and loading natively, which won't require
        translation before loading or after unloading.
        """
        if records_format_type == 'delimited':
            return 'bluelabs'
        else:
            return None

    # TODO: Should these be part of other interface?
    def load_failure_exception(self) -> Type[Exception]:
        return sqlalchemy.exc.InternalError

    @contextmanager
    def temporary_loadable_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        with TemporaryDirectory(prefix='temporary_loadable_directory_loc') as dirname:
            yield FilesystemDirectoryUrl(dirname)

    def known_supported_records_formats_for_load(self) -> List[BaseRecordsFormat]:
        """Candidates to look through when negotiating a common records format
        with a records source.  Will be looked through in order, so
        the better formats (higher fidelity and/or more efficient to
        process) should appear first in the list.
        """
        return []



class LoaderFromFileobj(NegotiatesLoadFormat, metaclass=ABCMeta):
    def load_from_fileobj(self, schema: str, table: str,
                          load_plan: RecordsLoadPlan, fileobj: IO[bytes]) -> Optional[int]:
        """Loads the data from the file stream provided.
        """
        raise NotImplementedError(f"load_from_fileobj not implemented for this database type")


class LoaderFromRecordsDirectory(NegotiatesLoadFormat, metaclass=ABCMeta):
    def best_scheme_to_load_from(self) -> str:
        return 'file'

    def load(self,
             schema: str,
             table: str,
             load_plan: RecordsLoadPlan,
             directory: RecordsDirectory) -> Optional[int]:
        """Loads the data from the data specified to the RecordsDirectory
        instance named 'directory'.  Guarantees a manifest file named
        'manifest' is written to the target directory pointing to the
        target records.

        Returns number of rows loaded (if database provides that
        info).
        """
        raise NotImplementedError(f"load not implemented for this database type")
