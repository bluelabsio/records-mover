import chardet
from contextlib import contextmanager
from . import RecordsHints, BootstrappingRecordsHints
from .csv_streamer import stream_csv, python_encoding_from_hint
import io
import logging
import csv
from .types import HintEncoding, HintRecordTerminator, HintQuoting
import pandas
from typing import Iterable, List, IO, Optional, Dict, Iterator, TYPE_CHECKING
if TYPE_CHECKING:
    from pandas.io.parsers import TextFileReader


logger = logging.getLogger(__name__)


def complain_on_unhandled_hints(fail_if_dont_understand: bool,
                                unhandled_hints: Iterable[str],
                                hints: RecordsHints) -> None:
    unhandled_bindings = [f"{k}={hints[k]}" for k in unhandled_hints]
    unhandled_bindings_str = ", ".join(unhandled_bindings)
    if len(unhandled_bindings) > 0:
        if fail_if_dont_understand:
            err = "Implement these records_format hints or try again with " +\
                f"fail_if_dont_understand=False': {unhandled_bindings_str}"
            raise NotImplementedError(err)
        else:
            logger.warning(f"Did not understand these hints: {unhandled_bindings_str}")


def cant_handle_hint(fail_if_cant_handle_hint: bool, hint_name: str, hints: RecordsHints) -> None:
    if not fail_if_cant_handle_hint:
        logger.warning("Ignoring hint {hint_name} = {hint_value}"
                       .format(hint_name=hint_name,
                               hint_value=repr(hints[hint_name])))
    else:
        raise NotImplementedError(f"Implement hint {hint_name}={repr(hints[hint_name])} " +
                                  "or try again with fail_if_cant_handle_hint=False")


python_date_format_from_hints = {
    'YYYY-MM-DD': '%Y-%m-%d',
    'MM/DD/YY': '%m/%d/%Y',
    'DD/MM/YY': '%d/%m/%Y',
}

python_time_format_from_hints = {
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

hint_compression_from_pandas = {
    # https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_csv.html
    # https://github.com/bluelabsio/knowledge/
    #    blob/master/Engineering/Architecture/JobDataExchange/output-design.md#hints
    'gzip': 'GZIP',
    'bz2': 'BZIP',
    None: None,
}


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
                         encoding_hint: HintEncoding) ->\
        Optional[HintRecordTerminator]:
    try:
        with rewound_fileobj(fileobj) as fileobj:
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


@contextmanager
def rewound_fileobj(fileobj: IO[bytes]) -> Iterator[IO[bytes]]:
    if getattr(fileobj, 'closed', None) is not None:
        closed = fileobj.closed
    if closed:
        logger.warning("Stream already closed")
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
                          encoding_hint: HintEncoding) -> RecordsHints:
    # https://docs.python.org/3/library/csv.html#csv.Sniffer
    try:
        with rewound_fileobj(fileobj) as fileobj:
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


def sniff_hints(fileobj: IO[bytes],
                initial_hints: BootstrappingRecordsHints) -> RecordsHints:
    if 'encoding' not in initial_hints:
        encoding_hint = sniff_encoding_hint(fileobj)
    else:
        encoding_hint = initial_hints['encoding']

    streaming_hints = initial_hints.copy()
    if encoding_hint is not None:
        streaming_hints['encoding'] = encoding_hint
    final_encoding_hint: HintEncoding = (encoding_hint or 'UTF8')
    other_inferred_csv_hints = {}
    record_terminator_hint: Optional[HintRecordTerminator] = None
    if 'record-terminator' in initial_hints:
        record_terminator_hint = initial_hints['record-terminator']
    else:
        record_terminator_hint = infer_newline_format(fileobj, final_encoding_hint)
    if record_terminator_hint is not None:
        other_inferred_csv_hints['record-terminator'] = record_terminator_hint
        python_inferred_hints = csv_hints_from_python(fileobj,
                                                      record_terminator_hint,
                                                      final_encoding_hint)
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
    return out  # type: ignore
