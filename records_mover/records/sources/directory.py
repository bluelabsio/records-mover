from .base import SupportsRecordsDirectory, SupportsToFileobjsSource
from .fileobjs import FileobjsSource
from contextlib import ExitStack
from ..records_directory import RecordsDirectory
from ..records_format import DelimitedRecordsFormat
from .. import PartialRecordsHints
from contextlib import contextmanager
from typing import Iterator, Optional
from ..processing_instructions import ProcessingInstructions
from ..records_format import BaseRecordsFormat
from ...url.resolver import UrlResolver


class RecordsDirectoryRecordsSource(SupportsRecordsDirectory,
                                    SupportsToFileobjsSource):
    def __init__(self,
                 directory: RecordsDirectory,
                 fail_if_dont_understand: bool,
                 url_resolver: UrlResolver,
                 override_hints: PartialRecordsHints={}) -> None:
        self.records_format = directory.load_format(fail_if_dont_understand=fail_if_dont_understand)
        if isinstance(self.records_format, DelimitedRecordsFormat):
            self.records_format = self.records_format.alter_hints(override_hints)
        self.directory = directory
        self.url_resolver = url_resolver

    def records_directory(self) -> RecordsDirectory:
        return self.directory

    @contextmanager
    def to_fileobjs_source(self,
                           processing_instructions: ProcessingInstructions,
                           records_format_if_possible: Optional[BaseRecordsFormat]=None)\
            -> Iterator[FileobjsSource]:

        all_urls = self.directory.manifest_entry_urls()

        locs = [self.url_resolver.file_url(url) for url in all_urls]

        with ExitStack() as stack:
            target_names_to_input_fileobjs = {
                loc.filename(): stack.enter_context(loc.open())
                for loc in locs
            }
            records_schema = self.directory.load_schema_json_obj()
            with FileobjsSource.infer_if_needed(target_names_to_input_fileobjs,
                                                processing_instructions=processing_instructions,
                                                records_format=self.records_format,
                                                records_schema=records_schema,
                                                initial_hints=None) as f:
                yield f
