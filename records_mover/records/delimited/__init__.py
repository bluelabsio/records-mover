__all__ = [
    'UntypedRecordsHints',
    'validate_partial_hints',
    'cant_handle_hint',
    'complain_on_unhandled_hints',
    'PartialRecordsHints',
    'ValidatedRecordsHints',
    'sniff_compression_from_url',
    'HintEncoding',
    'HintRecordTerminator',
    'HintFieldDelimiter',
    'HintQuoteChar',
    'HintQuoting',
    'HintEscape',
    'HintHeaderRow',
    'HintCompression',
    'HintDoublequote',
    'sniff_hints_from_fileobjs',
    'python_encoding_from_hint',
    'python_date_format_from_hints',
    'python_time_format_from_hints',
    'stream_csv',
]

"""RecordsHints are described as part of the overall `records format
documentation
<https://github.com/bluelabsio/knowledge/blob/master/Engineering/Architecture/JobDataExchange/output-design.md#hints>`_
Their goal in life is to both describe in detail how to write a
records file (especially delimited files, which are particularly
thorny in practice).

See the :py:meth:`records_mover.records.records_format.RecordsFormat`
for the other details that are typically provided along with records
hints.
"""

from .types import PartialRecordsHints, UntypedRecordsHints
from .validated_records_hints import ValidatedRecordsHints
from .hints import validate_partial_hints
from .utils import cant_handle_hint, complain_on_unhandled_hints
from .compression import sniff_compression_from_url
from .types import (
    HintEncoding, HintRecordTerminator,
    HintFieldDelimiter, HintQuoteChar,
    HintQuoting, HintEscape,
    HintHeaderRow, HintCompression,
    HintDoublequote,
)
from .sniff import sniff_hints_from_fileobjs
from .csv_streamer import stream_csv
from .conversions import (
    python_encoding_from_hint, python_date_format_from_hints, python_time_format_from_hints
)
