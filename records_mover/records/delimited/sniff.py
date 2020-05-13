import chardet
from contextlib import contextmanager
from . import RecordsHints, BootstrappingRecordsHints
from .csv_streamer import stream_csv, python_encoding_from_hint
import io
import logging
import csv
import gzip
import bz2
from .types import HintEncoding, HintRecordTerminator, HintQuoting, HintCompression
from .conversions import hint_compression_from_pandas, hint_encoding_from_chardet
import pandas
from typing import List, IO, Optional, Iterator, NoReturn, Dict, TYPE_CHECKING
if TYPE_CHECKING:
    from pandas.io.parsers import TextFileReader


logger = logging.getLogger(__name__)


@contextmanager
def rewound_fileobj(fileobj: IO[bytes]) -> Iterator[IO[bytes]]:
    if getattr(fileobj, 'closed', None) is not None:
        closed = fileobj.closed
    if closed:
        logger.warning("Stream already closed")
        # TODO: Am I catching this everywhere?
        raise OSError('Stream is already closed')
    if not fileobj.seekable():
        logger.warning("Stream not rewindable")
        raise OSError('Stream is not rewindable')
    original_position = fileobj.tell()
    fileobj.seek(0)
    try:
        yield fileobj
    finally:
        fileobj.seek(original_position)


def _assert_never(x: NoReturn) -> NoReturn:
    assert False, "Unhandled type: {}".format(type(x).__name__)


@contextmanager
def rewound_decompressed_fileobj(fileobj: IO[bytes],
                                 compression: HintCompression) -> Iterator[IO[bytes]]:
    with rewound_fileobj(fileobj) as fileobj_after_rewind:
        if compression is None:
            yield fileobj
        elif compression == 'GZIP':
            yield gzip.GzipFile(mode='rb', fileobj=fileobj_after_rewind)  # type: ignore
        elif compression == 'LZO':
            raise NotImplementedError
        elif compression == 'BZIP':
            yield bz2.BZ2File(mode='rb', filename=fileobj_after_rewind)
        else:
            _assert_never(compression)


def csv_hints_from_reader(reader: 'TextFileReader') -> RecordsHints:
    # https://github.com/pandas-dev/pandas/blob/master/pandas/io/parsers.py#L783
    # C parser:
    # https://github.com/pandas-dev/pandas/blob/e9b019b653d37146f9095bb0522525b3a8d9e386/pandas/io/parsers.py#L1903
    # Python parser:
    # https://github.com/pandas-dev/pandas/blob/e9b019b653d37146f9095bb0522525b3a8d9e386/pandas/io/parsers.py#L2253
    # Compression is definitely inferred:
    # https://github.com/pandas-dev/pandas/blob/e9b019b653d37146f9095bb0522525b3a8d9e386/pandas/io/parsers.py#L425
    # TODO: But I don't think I'm passing in 'infer' to csv_streamer...
    compression = reader._engine.compression

    return {
        # Note that at least 'escape', 'doublequote', 'quotechar',
        # 'encoding' and 'header-row' don't seem to be inferred in
        # practice, at least by the Python driver in Pandas
        'compression': hint_compression_from_pandas[compression],
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
                         encoding_hint: HintEncoding,
                         compression: HintCompression) ->\
        Optional[HintRecordTerminator]:
    try:
        with rewound_decompressed_fileobj(fileobj, compression) as fileobj:
            python_encoding = python_encoding_from_hint[encoding_hint]
            text_fileobj = io.TextIOWrapper(fileobj, encoding=python_encoding)
            try:
                if text_fileobj.newlines is None:  # ...and it almost certainly will be...
                    text_fileobj.readline()  # read enough to know newline format
                # https://www.python.org/dev/peps/pep-0278/
                if text_fileobj.newlines is not None:
                    logger.info(f"Inferred record terminator as {repr(text_fileobj.newlines)}")
                    return str(text_fileobj.newlines)
                else:
                    logger.warning("Python could not determine newline format of file.")
                    return None
            finally:
                text_fileobj.detach()
    except OSError:
        logger.warning("Assuming UNIX newline format, as stream is not rewindable")
        return None


def sniff_encoding_hint(fileobj: IO[bytes]) -> Optional[HintEncoding]:
    try:
        with rewound_fileobj(fileobj) as fileobj:
            detector = chardet.UniversalDetector()
            while True:
                chunksize = 512
                chunk = fileobj.read(chunksize)
                detector.feed(chunk)
                if detector.done or len(chunk) < chunksize:
                    break
            detector.close()
            assert detector.result is not None
            if 'encoding' in detector.result:
                chardet_encoding = detector.result['encoding']
                if chardet_encoding in hint_encoding_from_chardet:
                    return hint_encoding_from_chardet[chardet_encoding]
                else:
                    logger.warning("Got unrecognized encoding from chardet "
                                   f"sniffing: {detector.result}")
                    return None
            else:
                logger.warning(f"Unable to sniff file encoding using chardet: {detector.result}")
                return None
    except OSError:
        logger.warning("Could not use chardet to detect encoding, as stream is not rewindable")
        return None


def csv_hints_from_python(fileobj: IO[bytes],
                          record_terminator_hint: Optional[HintRecordTerminator],
                          encoding_hint: HintEncoding,
                          compression: HintCompression) -> RecordsHints:
    # https://docs.python.org/3/library/csv.html#csv.Sniffer
    try:
        with rewound_decompressed_fileobj(fileobj,
                                          compression) as fileobj:
            # Sniffer tries to determine quotechar, doublequote,
            # delimiter, skipinitialspace.  does not try to determine
            # lineterminator.
            # https://github.com/python/cpython/blob/master/Lib/csv.py#L165
            python_encoding = python_encoding_from_hint[encoding_hint]
            #
            # TextIOWrapper can only handle standard newline types:
            #
            # https://docs.python.org/3/library/io.html#io.TextIOWrapper
            #
            if record_terminator_hint not in [None, '\n', '\r', '\r\n']:
                logger.info("Unable to infer file with non-standard newlines "
                            f"using Python csv.Sniffer {repr(record_terminator_hint)}.")
                return {}
            text_fileobj = io.TextIOWrapper(fileobj,
                                            encoding=python_encoding,
                                            newline=record_terminator_hint)
            try:
                # TODO: How to get 1024?  processing instructions?
                sniffer = csv.Sniffer()
                sample = text_fileobj.read(1024)
                #
                # the CSV sniffer's has_header() method seems to only
                # cope with DOS and UNIX newlines, not Mac.  So let's give it
                # UNIX newlines if we know enough to translate, since
                # we're not using it to sniff newline format anyway.
                #
                if record_terminator_hint is not None and record_terminator_hint != '\n':
                    sample_with_unix_newlines = sample.replace(record_terminator_hint, '\n')
                else:
                    sample_with_unix_newlines = sample
                dialect = sniffer.sniff(sample_with_unix_newlines)
                header_row = sniffer.has_header(sample_with_unix_newlines)
                out: RecordsHints = {
                    'doublequote': dialect.doublequote,
                    'field-delimiter': dialect.delimiter,
                    'quotechar': dialect.quotechar,
                    'header-row': header_row,
                }
                logger.info(f"Python csv.Dialect sniffed: {out}")
                return out
            finally:
                text_fileobj.detach()
    except OSError:
        logger.warning("Could not use Python's csv library to detect hints, "
                       "as stream is not rewindable")
        return {}


def csv_hints_from_pandas(fileobj: IO[bytes],
                          streaming_hints: BootstrappingRecordsHints) -> RecordsHints:
    def attempt_parse(quoting: HintQuoting) -> RecordsHints:
        with rewound_fileobj(fileobj) as fresh_fileobj:
            current_hints = streaming_hints.copy()
            current_hints['quoting'] = quoting
            logger.info(f"Attempting to parse with quoting: {quoting}")
            with stream_csv(fresh_fileobj, current_hints) as reader:
                return {
                    **csv_hints_from_reader(reader),
                    'quoting': quoting
                }

    if 'quoting' in streaming_hints:
        return attempt_parse(streaming_hints['quoting'])
    else:
        # Pandas seems to parse quoting=minimal files just fine when
        # you pass in quoting=all, making this technique useless to
        # distinguish between minimal/nonnumeric/all, so we'll only
        # try None and minimal here.
        try:
            return attempt_parse(quoting='minimal')
        except (pandas.errors.ParserError, pandas.errors.EmptyDataError):
            return attempt_parse(quoting=None)


def sniff_compression_hint(fileobj: IO[bytes]) -> HintCompression:
    print(f'Sniffing compression')
    with rewound_fileobj(fileobj) as fileobj_rewound:
        # https://stackoverflow.com/a/13044946/9795956
        magic_dict: Dict[bytes, HintCompression] = {
            b"\x1f\x8b\x08": "GZIP",
            b"\x42\x5a\x68": "BZIP",
            # "\x50\x4b\x03\x04": "zip"
        }

        max_len = max(len(x) for x in magic_dict)

        file_start = fileobj_rewound.read(max_len)
        for magic, filetype in magic_dict.items():
            if file_start.startswith(magic):
                return filetype
        return None


def sniff_hints(fileobj: IO[bytes],
                initial_hints: BootstrappingRecordsHints) -> RecordsHints:
    if 'compression' in initial_hints:
        compression_hint = initial_hints['compression']
    else:
        compression_hint = sniff_compression_hint(fileobj)
    if 'encoding' not in initial_hints:
        encoding_hint = sniff_encoding_hint(fileobj)
    else:
        encoding_hint = initial_hints['encoding']

    streaming_hints = initial_hints.copy()
    streaming_hints['compression'] = compression_hint
    if encoding_hint is not None:
        streaming_hints['encoding'] = encoding_hint
    final_encoding_hint: HintEncoding = (encoding_hint or 'UTF8')
    other_inferred_csv_hints = {}
    record_terminator_hint: Optional[HintRecordTerminator] = None
    if 'record-terminator' in initial_hints:
        record_terminator_hint = initial_hints['record-terminator']
    else:
        record_terminator_hint = infer_newline_format(fileobj, final_encoding_hint,
                                                      compression_hint)
    if record_terminator_hint is not None:
        other_inferred_csv_hints['record-terminator'] = record_terminator_hint
        python_inferred_hints = csv_hints_from_python(fileobj,
                                                      record_terminator_hint,
                                                      final_encoding_hint,
                                                      compression_hint)
    streaming_hints.update(python_inferred_hints)  # type: ignore
    pandas_inferred_hints = csv_hints_from_pandas(fileobj, streaming_hints)
    out = {
        **pandas_inferred_hints,  # type: ignore
        **python_inferred_hints,  # type: ignore
        'encoding': final_encoding_hint,
        **other_inferred_csv_hints,  # type: ignore
        **initial_hints  # type: ignore
    }
    logger.info(f"Inferred hints from combined sources: {out}")
    return out
