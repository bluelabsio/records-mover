from .hint import LiteralHint, StringHint
import chardet
from .types import (
    RecordsHints, BootstrappingRecordsHints, HintHeaderRow, HintCompression, HintQuoting,
    HintDoublequote, HintEscape, HintEncoding, HintDateFormat, HintTimeOnlyFormat,
    HintDateTimeFormatTz, HintDateTimeFormat
)
from .csv_streamer import stream_csv, python_encoding_from_hint
import io
import logging
from .types import MutableRecordsHints
from typing import Iterable, List, IO, Optional, Dict, Union, TYPE_CHECKING
from typing_extensions import Literal
if TYPE_CHECKING:
    from pandas.io.parsers import TextFileReader


logger = logging.getLogger(__name__)


# TODO: after merging sniffing improvements, move to a new file
# TODO: this shouldn't have to be a union
python_date_format_from_hints: Dict[Union[HintDateFormat, Literal['DD/MM/YY']], str] = {
    'YYYY-MM-DD': '%Y-%m-%d',
    'MM/DD/YY': '%m/%d/%Y',
    'DD/MM/YY': '%d/%m/%Y',
}

python_time_format_from_hints: Dict[HintTimeOnlyFormat, str] = {
    'HH24:MI:SS': '%H:%M:%S',
    'HH12:MI AM': '%I:%M:%S %p',
}

hint_encoding_from_pandas: Dict[str, HintEncoding] = {
    'utf-8': 'UTF8',
    'utf-16': 'UTF16',
    'utf-16-le': 'UTF16LE',
    'utf-16-be': 'UTF16BE',
}

hint_encoding_from_chardet: Dict[str, HintEncoding] = {
    'UTF-8-SIG': 'UTF8BOM',
    'UTF-16': 'UTF16',
    'ISO-8859-1': 'LATIN1',
    # For some reason this is lowercase:
    #  https://github.com/chardet/chardet/blob/17218468eb16b7d0068bce7e4d20bac70f0bf555/chardet/utf8prober.py#L51
    'utf-8': 'UTF8',
    # But let's be ready if they change their minds:
    'UTF-8': 'UTF8',
    'Windows-1252': 'CP1252',
    # even if the only data it saw was in ASCII, let's be ready to see more
    'ascii': 'UTF8',
}

hint_compression_from_pandas: Dict[Optional[str], HintCompression] = {
    # https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_csv.html
    # https://github.com/bluelabsio/knowledge/
    #    blob/master/Engineering/Architecture/JobDataExchange/output-design.md#hints
    'gzip': 'GZIP',
    'bz2': 'BZIP',
    None: None,
}
