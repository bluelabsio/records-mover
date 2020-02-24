import csv
import io
from contextlib import contextmanager
from records_mover.records import BootstrappingRecordsHints
from typing import Union, IO, Optional, Iterator, TYPE_CHECKING
if TYPE_CHECKING:
    from pandas.io.parsers import TextFileReader  # noqa


pandas_compression_from_hint = {
    'GZIP': 'gzip',
    'BZIP': 'bz2',
    None: None,
}


pandas_quoting_from_hint = {
    'minimal': csv.QUOTE_MINIMAL,
    'all': csv.QUOTE_ALL,
    'nonnumeric': csv.QUOTE_NONNUMERIC,
    None: csv.QUOTE_NONE
}

python_encoding_from_hint = {
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


@contextmanager
def stream_csv(filepath_or_buffer: Union[str, IO[bytes]],
               hints: BootstrappingRecordsHints)\
               -> Iterator['TextFileReader']:
    """Returns a context manager that can be used to generate a full or
    partial dataframe from a CSV.  If partial, it will not read the
    entire CSV file into memory."""

    from pandas import read_csv

    header_row = hints.get('header-row')
    header: Union[str, int, None]
    if header_row is None:
        # we weren't told...
        header = 'infer'
    elif header_row:
        # there's a header row in the file, so tell Pandas that row 0
        # is the headers:
        header = 0
    else:
        # there's no header row in the file
        header = None
    compression_hint: Optional[str] = hints.get('compression')
    encoding_hint = hints.get('encoding', 'UTF8')
    kwargs = {
        'sep': hints.get('field-delimiter', ','),
        'encoding': python_encoding_from_hint.get(encoding_hint, encoding_hint),
        'header': header,
        'compression': pandas_compression_from_hint[compression_hint],
        'escapechar': hints.get('escape'),
        'prefix': 'untitled_',
        'iterator': True,
        'engine': 'python'
    }
    if 'quoting' in hints:
        quoting: Optional[str] = hints['quoting']
        kwargs['quoting'] = pandas_quoting_from_hint[quoting]
    # The streaming code from pandas demands a text stream if we're
    # dealing with an uncompressed CSV file, and a binary stream
    # if we're dealing with compressed file.
    if isinstance(filepath_or_buffer, str):
        # Send a local filename...
        out = None
        try:
            out = read_csv(filepath_or_buffer, **kwargs)
            yield out
        finally:
            if out is not None:
                out.close()
    elif compression_hint is None:
        hint_encoding: str = hints.get('encoding')  # type: ignore
        encoding = python_encoding_from_hint.get(hint_encoding, hint_encoding)
        text_fileobj = io.TextIOWrapper(filepath_or_buffer, encoding=encoding)
        out = None
        try:
            out = read_csv(text_fileobj, **kwargs)
            yield out
        finally:
            if out is not None:
                out.close()
            text_fileobj.detach()
    else:
        # Pass as a binary file
        out = None
        try:
            out = read_csv(filepath_or_buffer, **kwargs)
            yield out
        finally:
            if out is not None:
                out.close()
