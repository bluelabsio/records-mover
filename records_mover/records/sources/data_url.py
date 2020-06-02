from .fileobjs import FileobjsSource
from urllib.parse import urlparse
from .base import SupportsToFileobjsSource
from contextlib import contextmanager
from ..delimited import sniff_compression_from_url
from ..processing_instructions import ProcessingInstructions
from ...url.resolver import UrlResolver
from ..records_format import BaseRecordsFormat
from ..schema import RecordsSchema
from .. import PartialRecordsHints
import logging
from typing import Optional, Iterator

logger = logging.getLogger(__name__)


class DataUrlRecordsSource(SupportsToFileobjsSource):
    def __init__(self,
                 input_url: str,
                 url_resolver: UrlResolver,
                 records_format: Optional[BaseRecordsFormat]=None,
                 records_schema: Optional[RecordsSchema]=None,
                 initial_hints: Optional[PartialRecordsHints]=None) -> None:
        self.input_url = input_url
        self.url_resolver = url_resolver
        self.records_format = records_format
        self.records_schema = records_schema
        self.initial_hints = initial_hints
        if self.initial_hints is None:
            self.initial_hints = {}
        if 'compression' not in self.initial_hints:
            # if we end up sniffing hints, we can start out assuming
            # compression type based on the filename.
            self.initial_hints['compression'] = sniff_compression_from_url(self.input_url)

    @contextmanager
    def to_fileobjs_source(self,
                           processing_instructions: ProcessingInstructions,
                           records_format_if_possible: Optional[BaseRecordsFormat]=None)\
            -> Iterator['FileobjsSource']:
        """Convert current source to a FileObjsSource and present it in a context manager"""
        with self.url_resolver.file_url(self.input_url).open() as fileobj:
            input_url_obj = urlparse(self.input_url)
            path = input_url_obj.path
            filename = path.split('/')[-1]
            with FileobjsSource.\
                infer_if_needed(target_names_to_input_fileobjs={filename: fileobj},
                                records_format=self.records_format,
                                records_schema=self.records_schema,
                                processing_instructions=processing_instructions,
                                initial_hints=self.initial_hints) as fileobjs_source:
                yield fileobjs_source

    def __str__(self) -> str:
        return f"{type(self).__name__}({self.records_format})"
