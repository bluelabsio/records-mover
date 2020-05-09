from .hint import LiteralHint, StringHint
import chardet
from .types import (
    RecordsHints, BootstrappingRecordsHints, HintHeaderRow, HintCompression, HintQuoting,
    HintDoublequote, HintEscape, HintEncoding, HintDateFormat, HintTimeOnlyFormat,
    HintDateTimeFormatTz, HintDateTimeFormat
)
from .csv_streamer import stream_csv
from .conversions import python_encoding_from_hint
import io
import logging
from .types import MutableRecordsHints
from .conversions import (
    hint_compression_from_pandas,
    hint_encoding_from_pandas,
    hint_encoding_from_chardet
)
from typing import Iterable, List, IO, Optional, Dict, TYPE_CHECKING
if TYPE_CHECKING:
    from pandas.io.parsers import TextFileReader


logger = logging.getLogger(__name__)


def csv_hints_from_reader(reader: 'TextFileReader') -> RecordsHints:
    header = reader._engine.header
    quotechar = reader._engine.data.dialect.quotechar
    delimiter = reader._engine.data.dialect.delimiter
    escape_char = reader._engine.data.dialect.escapechar
    compression = reader._engine.compression
    encoding = reader._engine.encoding
    doublequote = reader._engine.doublequote

    return {
        'header-row': True if header is not None else False,
        'field-delimiter': delimiter,
        'compression': hint_compression_from_pandas[compression],
        'quotechar': quotechar,
        'doublequote': doublequote,
        'escape': escape_char,
        'encoding': hint_encoding_from_pandas.get(encoding, encoding),
        'dateformat': 'YYYY-MM-DD',
        'timeonlyformat': 'HH12:MI AM',
        'datetimeformat': 'YYYY-MM-DD HH:MI:SS',
        'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
    }


def sniff_hints_from_fileobjs(fileobjs: List[IO[bytes]],
                              initial_hints: BootstrappingRecordsHints) -> RecordsHints:
    if len(fileobjs) != 1:
        # https://app.asana.com/0/53283930106309/1131698268455054
        raise NotImplementedError('Cannot currently sniff hints from mulitple '
                                  'files--please provide hints')
    fileobj = fileobjs[0]
    if not fileobj.seekable():
        raise NotImplementedError('Cannot currently sniff hints from a pure stream--'
                                  'please save file to disk and load from there or '
                                  'provide explicit records format information')
    hints = sniff_hints(fileobj, initial_hints=initial_hints)
    fileobj.seek(0)
    return hints


def infer_newline_format(fileobj: IO[bytes],
                         inferred_hints: MutableRecordsHints,
                         encoding_hint: str) -> None:
    closed = False
    if getattr(fileobj, 'closed', None) is not None:
        closed = fileobj.closed
    if closed or not fileobj.seekable():
        logger.warning("Assuming UNIX newline format, as stream is not rewindable")
        return
    python_encoding = python_encoding_from_hint[encoding_hint]  # type: ignore
    original_position = fileobj.tell()
    fileobj.seek(0)
    text_fileobj = io.TextIOWrapper(fileobj, encoding=python_encoding)
    if text_fileobj.newlines is None:  # ...and it almost certainly will be...
        text_fileobj.readline()  # read enough to know newline format
    # https://www.python.org/dev/peps/pep-0278/
    if text_fileobj.newlines is not None:
        inferred_hints['record-terminator'] = str(text_fileobj.newlines)
        logger.info(f"Inferred record terminator as {repr(text_fileobj.newlines)}")
    else:
        logger.warning("Python could not determine newline format of file.")
    text_fileobj.detach()
    fileobj.seek(original_position)


def other_inferred_csv_hints(fileobj: IO[bytes],
                             encoding_hint: str) -> RecordsHints:
    inferred_hints: MutableRecordsHints = {}
    infer_newline_format(fileobj, inferred_hints, encoding_hint)
    return inferred_hints


def sniff_encoding_hint(fileobj: IO[bytes]) -> Optional[HintEncoding]:
    if getattr(fileobj, 'closed', None) is not None:
        closed = fileobj.closed
    if closed or not fileobj.seekable():
        logger.warning("Could not use chardet to detect encoding, as stream is not rewindable")
        return None
    original_position = fileobj.tell()
    fileobj.seek(0)
    detector = chardet.UniversalDetector()
    while True:
        chunksize = 512
        chunk = fileobj.read(chunksize)
        detector.feed(chunk)
        if detector.done or len(chunk) < chunksize:
            break
    detector.close()
    fileobj.seek(original_position)
    assert detector.result is not None
    if 'encoding' in detector.result:
        chardet_encoding = detector.result['encoding']
        if chardet_encoding in hint_encoding_from_chardet:
            return hint_encoding_from_chardet[chardet_encoding]
        else:
            logger.warning(f"Got unrecognized encoding from chardet sniffing: {detector.result}")
            return None
    else:
        logger.warning(f"Unable to sniff file encoding using chardet: {detector.result}")
        return None


def sniff_hints(fileobj: IO[bytes],
                initial_hints: BootstrappingRecordsHints) -> RecordsHints:
    if 'encoding' not in initial_hints:
        encoding_hint = sniff_encoding_hint(fileobj)
    else:
        encoding_hint = initial_hints['encoding']

    streaming_hints = initial_hints.copy()
    if encoding_hint is not None:
        streaming_hints['encoding'] = encoding_hint
    with stream_csv(fileobj, streaming_hints) as reader:
        # overwrite hints from reader with user-specified values, as
        # the reader isn't smart enough to remember things like which
        # quoting setting it was told to use...
        pandas_inferred_hints = csv_hints_from_reader(reader)
        final_encoding_hint: str = (encoding_hint or  # type: ignore
                                    pandas_inferred_hints['encoding'])
        return {**pandas_inferred_hints,
                'encoding': final_encoding_hint,
                **other_inferred_csv_hints(fileobj, final_encoding_hint),
                **initial_hints}  # type: ignore
