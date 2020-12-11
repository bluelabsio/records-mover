from .types import (
    HintCompression, HintEncoding, HintDateFormat, HintTimeOnlyFormat, HintQuoting,
    HintDateTimeFormat, HintDateTimeFormatTz
)
import logging
import csv
from typing import Optional, Dict, Union


logger = logging.getLogger(__name__)

pandas_compression_from_hint: Dict[HintCompression, Optional[str]] = {
    'GZIP': 'gzip',
    'BZIP': 'bz2',
    None: None,
}

pandas_quoting_from_hint: Dict[HintQuoting, int] = {
    'minimal': csv.QUOTE_MINIMAL,
    'all': csv.QUOTE_ALL,
    'nonnumeric': csv.QUOTE_NONNUMERIC,
    None: csv.QUOTE_NONE
}

python_encoding_from_hint: Dict[Optional[HintEncoding], str] = {
    # valid python names: https://docs.python.org/3/library/codecs.html#standard-encodings
    # valid hints names: https://github.com/bluelabsio/knowledge/blob/master/Engineering/
    #    Architecture/JobDataExchange/output-design.md
    'UTF8': 'utf-8',
    'UTF16': 'utf-16',
    'UTF16LE': 'utf-16-le',
    'UTF16BE': 'utf-16-be',
    'LATIN1': 'latin_1',
    'CP1252': 'cp1252',
    'UTF8BOM': 'utf-8-sig',
    # Python will auto-detect UTF-16 with a BOM, but not UTF-8
    'UTF16BOM': 'utf-16',
}


def hint_to_python_strftime(hint: Union[HintDateFormat,
                                        HintDateTimeFormat,
                                        HintDateTimeFormatTz,
                                        HintTimeOnlyFormat]) -> str:
    if 'AM' in hint:
        hour_specifier = '%I'
    else:
        hour_specifier = '%H'
    return hint\
        .replace('YYYY', '%Y')\
        .replace('YY', '%y')\
        .replace('MM', '%m')\
        .replace('DD', '%d')\
        .replace('HH24', '%H')\
        .replace('HH12', '%I')\
        .replace('HH', hour_specifier)\
        .replace('MI', '%M')\
        .replace('SS', '%S.%f')\
        .replace('OF', '%z')\
        .replace('AM', '%p')


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
