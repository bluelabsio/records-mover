from records_mover.mover_types import _ensure_all_cases_covered
from records_mover.records.records_types import RecordsFormatType
from .records_format import (
    BaseRecordsFormat, DelimitedRecordsFormat, ParquetRecordsFormat, AvroRecordsFormat
)
from ..url.base import BaseDirectoryUrl, BaseFileUrl
from .processing_instructions import ProcessingInstructions
from .delimited import PartialRecordsHints
from typing import cast
import logging

logger = logging.getLogger(__name__)


class RecordsFormatFile:
    def __init__(self, records_loc: BaseDirectoryUrl) -> None:
        self.records_loc = records_loc

    def load_format(self, fail_if_dont_understand: bool) -> BaseRecordsFormat:
        prefix = '_format_'
        matching_locs = self.records_loc.files_matching_prefix(prefix)
        if len(matching_locs) != 1:
            raise TypeError(f"_format file not found in bucket under {prefix}*")
        format_loc = matching_locs[0]
        format_type = format_loc.filename()[len(prefix):]
        # cast so we can ensure at build-time we have covered all
        # cases below
        format_type = cast(RecordsFormatType, format_type)
        if format_type == 'delimited':
            return self.load_delimited_format(format_loc, fail_if_dont_understand)
        elif format_type == 'parquet':
            return ParquetRecordsFormat()
        elif format_type == 'avro':
            return AvroRecordsFormat()
        else:
            _ensure_all_cases_covered(format_type)
            raise TypeError(f"Format type {format_type} not yet supported in this library")

    def load_delimited_format(self, format_loc: BaseFileUrl,
                              fail_if_dont_understand: bool) -> BaseRecordsFormat:
        data = format_loc.json_contents()
        variant = None
        hints: PartialRecordsHints = {}
        # For simple form (such as with avro or parquet), this file MUST be empty.
        if data is None:
            # For detailed form (such as delimited), it MUST contain
            # the same JSON as in the format field in the
            # configuration.
            raise TypeError(f"Detailed format information must be provided in {format_loc.url} "
                            f"for type delimited")
        if not isinstance(data, dict):
            raise TypeError(f"{format_loc.url} does not contain a JSON object as expected")
        if 'variant' not in data:
            raise TypeError(f"variant not specified in {format_loc.url}")
        potential_variant = data['variant']
        if not isinstance(potential_variant, str):
            raise TypeError(f"Invalid variant specified in {format_loc.url}: "
                            f"{potential_variant}")
        variant = potential_variant
        if 'hints' in data:
            hints = data['hints']
        processing_instructions =\
            ProcessingInstructions(fail_if_dont_understand=fail_if_dont_understand)
        return DelimitedRecordsFormat(variant=variant, hints=hints,
                                      processing_instructions=processing_instructions)

    def save_format(self, records_format: BaseRecordsFormat) -> None:
        file_loc = self.records_loc.file_in_this_directory(f"_format_{records_format.format_type}")
        contents = records_format.json()
        logger.info(f"Storing format info into {file_loc.url}")
        file_loc.store_string(contents)
