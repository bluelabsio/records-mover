from ..url.resolver import UrlResolver
from ..records.records_format import BaseRecordsFormat
from ..records.load_plan import RecordsLoadPlan
from ..records.records_directory import RecordsDirectory
from ..url.filesystem import FilesystemDirectoryUrl
from contextlib import contextmanager
from ..url.base import BaseDirectoryUrl
from tempfile import TemporaryDirectory
from abc import ABCMeta, abstractmethod
from typing import Optional, Type, Iterator, IO, List
import sqlalchemy
import logging


logger = logging.getLogger(__name__)


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
        instance named 'directory'.

        Returns number of rows loaded (if database provides that
        info)."""
        ...

    @abstractmethod
    def can_load_this_format(self, source_records_format: BaseRecordsFormat) -> bool:
        """Return true if the specified format is compatible with the load()
        method"""
        ...

    def temporary_loadable_directory_scheme(self) -> str:
        """If we need to provide a temporary location that this database can
        load from with the temporary_loadable_directory_loc() method,, what
        URL scheme will be used?"""
        return 'file'

    @contextmanager
    def temporary_loadable_directory_loc(self) -> Iterator[BaseDirectoryUrl]:
        """Provide a temporary directory which can be used for bulk import to
        this database and clean it up when done"""
        with TemporaryDirectory(prefix='temporary_loadable_directory_loc') as dirname:
            yield FilesystemDirectoryUrl(dirname)

    def has_temporary_loadable_directory_loc(self) -> bool:
        """Returns True if a temporary directory can be provided by
        temporary_loadable_directory_loc()"""
        # The default implementation uses the local filesystem where
        # Records Mover runs, and we assume we can make temporary
        # files.
        return True

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
        """Loads the data from the file stream provided and append to the existing table."""
        ...

    def load_from_records_directory_via_fileobj(self,
                                                schema: str,
                                                table: str,
                                                load_plan: RecordsLoadPlan,
                                                directory: RecordsDirectory) -> Optional[int]:
        all_urls = directory.manifest_entry_urls()

        total_rows = None
        for url in all_urls:
            loc = self.url_resolver.file_url(url)
            with loc.open() as f:
                logger.info(f"Loading {url} into {schema}.{table}...")
                out = self.load_from_fileobj(schema, table, load_plan, f)
                if out is not None:
                    if total_rows is None:
                        total_rows = 0
                    total_rows += out
        return total_rows

    def load(self,
             schema: str,
             table: str,
             load_plan: RecordsLoadPlan,
             directory: RecordsDirectory) -> Optional[int]:
        # Implement load() in terms of load_from_fileobj() by default
        return self.load_from_records_directory_via_fileobj(schema=schema,
                                                            table=table,
                                                            load_plan=load_plan,
                                                            directory=directory)
