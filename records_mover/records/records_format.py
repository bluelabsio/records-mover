import logging
from records_mover.mover_types import _ensure_all_cases_covered
from .processing_instructions import ProcessingInstructions
from . import PartialRecordsHints, UntypedRecordsHints
from .base_records_format import BaseRecordsFormat
from typing import Mapping, Optional, TYPE_CHECKING
from .delimited import ValidatedRecordsHints
if TYPE_CHECKING:
    from . import RecordsFormatType  # noqa

logger = logging.getLogger(__name__)


class AvroRecordsFormat(BaseRecordsFormat):
    "Describes records files in `Avro <https://avro.apache.org/>`_ format"

    def __init__(self) -> None:
        "Create a new instance of AvroRecordsFormat"
        self.format_type = 'avro'

    def __str__(self) -> str:
        return "AvroRecordsFormat"

    def __repr__(self) -> str:
        return str(self)

    def generate_filename(self, basename: str) -> str:
        return f"{basename}.avro"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, AvroRecordsFormat):
            return True
        return False


class ParquetRecordsFormat(BaseRecordsFormat):
    "Describes records files in `Parquet <https://parquet.apache.org/>`_ format"

    def __init__(self) -> None:
        "Create a new instance of ParquetRecordsFormat"
        self.format_type = 'parquet'

    def __str__(self) -> str:
        return "ParquetRecordsFormat"

    def __repr__(self) -> str:
        return str(self)

    def generate_filename(self, basename: str) -> str:
        return f"{basename}.parquet"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ParquetRecordsFormat):
            return True
        return False


class DelimitedRecordsFormat(BaseRecordsFormat):
    "Describes records data files in delimited (CSV) format."

    variant: str
    # The strong type for 'hints' in the constructor is for IDE
    # support--we don't actually trust input to this class to be well-typed,
    # as it may come from public interface consumers.
    hints: UntypedRecordsHints
    """Stores the full set of hints describing the format, combining both
    the default hints for the variant and any hint overrides provided
    in the constructor"""

    def __init__(self,
                 variant: str='bluelabs',
                 hints: PartialRecordsHints={},
                 processing_instructions: ProcessingInstructions=ProcessingInstructions()) -> None:
        """See the `records format documentation
        <https://github.com/bluelabsio/records-mover/blob/master/docs/RECORDS_SPEC.md#hints>`_
        for full details on parameters.

        :param variant: For a given type (especially delimited), describe the subtype of the
           format.  For 'delimited', valid values include 'dumb', 'csv', 'bluelabs', 'bigquery'
           and 'vertica'.
        :param hints: Dictionary of names of delimited hints mapping to their values.  See the
           `records format specification
           <https://github.com/bluelabsio/records-mover/blob/master/docs/RECORDS_SPEC.md>`_
           for hints and valid values.
        :param processing_instructions: Directives on how to handle different situations when
           processing files.
        :type processing_instructions: records_mover.records.ProcessingInstructions
        """
        self.format_type = 'delimited'
        self.variant = variant
        self.custom_hints = hints
        self.add_hints_from_variant(provided_hints=hints,
                                    processing_instructions=processing_instructions)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DelimitedRecordsFormat):
            return (self.format_type == other.format_type and
                    self.variant == other.variant and
                    self.hints == other.hints)
        return False

    def alter_hints(self, new_hints: UntypedRecordsHints) ->\
            'DelimitedRecordsFormat':
        input_hints = dict(self.custom_hints)  # make copy
        input_hints.update(new_hints)
        return DelimitedRecordsFormat(variant=self.variant,
                                      hints=input_hints)  # type: ignore

    def alter_variant(self, variant: str) -> 'DelimitedRecordsFormat':
        return DelimitedRecordsFormat(variant=variant,
                                      hints=self.custom_hints)

    def base_hints_from_variant(self,
                                fail_if_dont_understand: bool = True) -> PartialRecordsHints:
        hint_defaults: PartialRecordsHints = {
            'header-row': False,
            'field-delimiter': ',',
            'record-terminator': "\n",
            'compression': 'GZIP',
            'quoting': None,
            'quotechar': '"',
            'doublequote': False,
            'escape': None,
            'encoding': 'UTF8',
            'dateformat': 'YYYY-MM-DD',
            'timeonlyformat': 'HH24:MI:SS',
            'datetimeformat': 'YYYY-MM-DD HH:MI:SS',
            'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
        }
        combined_hints: PartialRecordsHints = dict(hint_defaults)  # type: ignore
        format_driven_hints: PartialRecordsHints = {}  # noqa
        if self.variant == 'dumb':
            format_driven_hints['field-delimiter'] = ','
            format_driven_hints['record-terminator'] = "\n"
            format_driven_hints['quoting'] = None
            format_driven_hints['doublequote'] = False
        elif self.variant == 'csv':
            format_driven_hints['field-delimiter'] = ','
            format_driven_hints['quoting'] = 'minimal'
            format_driven_hints['doublequote'] = True
            format_driven_hints['quotechar'] = '"'
            format_driven_hints['dateformat'] = 'MM/DD/YY'
            format_driven_hints['datetimeformat'] = 'MM/DD/YY HH24:MI'
            format_driven_hints['datetimeformattz'] = 'MM/DD/YY HH24:MI'
            format_driven_hints['timeonlyformat'] = 'HH24:MI:SS'
            format_driven_hints['header-row'] = True
        elif self.variant == 'bigquery':
            format_driven_hints['field-delimiter'] = ','
            format_driven_hints['quoting'] = 'minimal'
            format_driven_hints['doublequote'] = True
            format_driven_hints['quotechar'] = '"'
            format_driven_hints['header-row'] = True
            format_driven_hints['datetimeformat'] = 'YYYY-MM-DD HH:MI:SS'
            format_driven_hints['datetimeformattz'] = 'YYYY-MM-DD HH:MI:SS'
        elif self.variant == 'bluelabs':
            format_driven_hints['field-delimiter'] = ','
            format_driven_hints['escape'] = '\\'
            format_driven_hints['quoting'] = None
            format_driven_hints['doublequote'] = False
            format_driven_hints['dateformat'] = 'YYYY-MM-DD'
            format_driven_hints['datetimeformat'] = 'YYYY-MM-DD HH24:MI:SS'
        elif self.variant == 'vertica':
            format_driven_hints['field-delimiter'] = '\001'
            format_driven_hints['record-terminator'] = '\002'
            format_driven_hints['quoting'] = None
            format_driven_hints['doublequote'] = False
            format_driven_hints['escape'] = None
            format_driven_hints['compression'] = None
            format_driven_hints['timeonlyformat'] = 'HH24:MI:SS'
        else:
            if fail_if_dont_understand:
                raise NotImplementedError("Implement delimited records format variant "
                                          f"{self.variant} or try "
                                          "again with fail_if_dont_understand=False")
            else:
                logger.warning("Ignoring delimited records format "
                               f"variant {self.variant}")
        combined_hints.update(format_driven_hints)
        return combined_hints

    def add_hints_from_variant(self,
                               provided_hints: PartialRecordsHints,
                               processing_instructions: ProcessingInstructions) -> None:
        combined_hints =\
            self.base_hints_from_variant(processing_instructions.fail_if_dont_understand)
        combined_hints.update(provided_hints)  # user's hints override those from the format
        self.hints = combined_hints

    def config(self) -> Optional[Mapping[str, object]]:
        contents_data = None
        if self.variant is not None or len(self.hints) != 0:
            contents_data = {
                'type': self.format_type,
                'variant': self.variant,
                'hints': self.hints
            }
        return contents_data

    def __str__(self) -> str:
        hints_from_variant = self.base_hints_from_variant()
        hint_overrides = {
            hint: v for hint, v in self.hints.items()
            if hint not in hints_from_variant or v != hints_from_variant[hint]  # type: ignore
        }
        if hint_overrides != {}:
            return f"DelimitedRecordsFormat({self.variant} - {hint_overrides})"
        else:
            return f"DelimitedRecordsFormat({self.variant})"

    def generate_filename(self, basename: str) -> str:
        compression_type = self.hints['compression']
        if compression_type is None:
            return f"{basename}.csv"
        elif compression_type == 'GZIP':
            return f"{basename}.csv.gz"
        elif compression_type == 'BZIP':
            return f"{basename}.csv.bz2"
        else:
            raise NotImplementedError("Teach me how to handle compression "
                                      f"type {compression_type}")

    def validate(self,
                 fail_if_cant_handle_hint: bool) -> ValidatedRecordsHints:
        return ValidatedRecordsHints.validate(self.hints,
                                              fail_if_cant_handle_hint=fail_if_cant_handle_hint)

    def __repr__(self) -> str:
        return str(self)


def RecordsFormat(format_type: 'RecordsFormatType' = 'delimited',
                  variant: str='bluelabs',
                  hints: PartialRecordsHints={},
                  processing_instructions:
                  ProcessingInstructions=ProcessingInstructions()) ->\
            'BaseRecordsFormat':
    if format_type == 'delimited':
        return DelimitedRecordsFormat(variant=variant,
                                      hints=hints,
                                      processing_instructions=processing_instructions)
    elif format_type == 'parquet':
        return ParquetRecordsFormat()
    elif format_type == 'avro':
        return AvroRecordsFormat()
    else:
        _ensure_all_cases_covered(format_type)
        raise NotImplementedError(f'Teach me to handle format type {format_type}')
