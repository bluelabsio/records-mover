from .fileobjs import FileobjsSource
from .base import SupportsToFileobjsSource
from contextlib import contextmanager
from ..schema import RecordsSchema
from ..processing_instructions import ProcessingInstructions
from ..records_format import BaseRecordsFormat
from records_mover.records.delimited import (
    UntypedRecordsHints, validate_partial_hints
)
import logging
from typing import Optional, Iterator, Mapping, IO

logger = logging.getLogger(__name__)


class UninferredFileobjsRecordsSource(SupportsToFileobjsSource):
    def __init__(self,
                 target_names_to_input_fileobjs: Mapping[str, IO[bytes]],
                 records_format: Optional[BaseRecordsFormat]=None,
                 records_schema: Optional[RecordsSchema]=None,
                 initial_hints: Optional[UntypedRecordsHints]=None) -> None:
        self.target_names_to_input_fileobjs = target_names_to_input_fileobjs
        self.records_format = records_format
        self.records_schema = records_schema
        self.initial_hints = initial_hints
        if self.initial_hints is None:
            self.initial_hints = {}

    @contextmanager
    def to_fileobjs_source(self,
                           processing_instructions: ProcessingInstructions,
                           records_format_if_possible: Optional[BaseRecordsFormat]=None)\
            -> Iterator['FileobjsSource']:
        """Convert current source to a FileObjsSource and present it in a context manager"""
        typed_hints = None
        if self.initial_hints is not None:
            typed_hints =\
                validate_partial_hints(self.initial_hints,
                                       fail_if_cant_handle_hint=processing_instructions.
                                       fail_if_cant_handle_hint)
        with FileobjsSource.\
                infer_if_needed(target_names_to_input_fileobjs=self.target_names_to_input_fileobjs,
                                records_format=self.records_format,
                                records_schema=self.records_schema,
                                processing_instructions=processing_instructions,
                                initial_hints=typed_hints) as fileobjs_source:
            yield fileobjs_source
