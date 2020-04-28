from contextlib import ExitStack
from ..url.resolver import UrlResolver
from ..records.records_format import BaseRecordsFormat
from ..utils.concat_files import ConcatFiles
from ..records.load_plan import RecordsLoadPlan
from ..records.records_directory import RecordsDirectory
from ..url.filesystem import FilesystemDirectoryUrl
from contextlib import contextmanager
from ..url.base import BaseDirectoryUrl
from tempfile import TemporaryDirectory
from abc import ABCMeta, abstractmethod
from typing import Optional, Type, Iterator, IO, List
import sqlalchemy


class LoaderFromRecordsDirectory(metaclass=ABCMeta):
    def load_failure_exception(self) -> Type[Exception]:
        return sqlalchemy.exc.InternalError

    def best_scheme_to_load_from(self) -> str:
        return 'file'

    @abstractmethod
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
        ...

    @abstractmethod
    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        """Return true if the specified format is compatible with the load()
        method"""
        ...

    @contextmanager
    def temporary_loadable_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        with TemporaryDirectory(prefix='temporary_loadable_directory_loc') as dirname:
            yield FilesystemDirectoryUrl(dirname)

    @abstractmethod
    def known_supported_records_formats_for_load(self) -> List[BaseRecordsFormat]:
        """Candidates to look through when negotiating a common records format
        with a records source.  Will be looked through in order, so
        the better formats (higher fidelity and/or more efficient to
        process) should appear first in the list.
        """
        ...


class LoaderFromFileobj(LoaderFromRecordsDirectory, metaclass=ABCMeta):
    url_resolver: UrlResolver

    @abstractmethod
    def load_from_fileobj(self, schema: str, table: str,
                          load_plan: RecordsLoadPlan, fileobj: IO[bytes]) -> Optional[int]:
        """Loads the data from the file stream provided.
        """
        ...

    # Implement load() in terms of load_from_fileobj()
    def load(self,
             schema: str,
             table: str,
             load_plan: RecordsLoadPlan,
             directory: RecordsDirectory) -> Optional[int]:
        all_urls = directory.manifest_entry_urls()

        locs = [self.url_resolver.file_url(url) for url in all_urls]
        fileobjs: List[IO[bytes]] = []
        with ExitStack() as stack:
            fileobjs = [stack.enter_context(loc.open()) for loc in locs]
            concatted_fileobj: IO[bytes] = ConcatFiles(fileobjs)  # type: ignore
            return self.load_from_fileobj(schema,
                                          table,
                                          load_plan,
                                          concatted_fileobj)
        return None  # make mypy happy
