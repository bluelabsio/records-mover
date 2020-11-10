import chardet
from contextlib import contextmanager
from . import PartialRecordsHints
from .csv_streamer import stream_csv, python_encoding_from_hint
import io
import csv
import gzip
import bz2
from .types import HintEncoding, HintRecordTerminator, HintQuoting, HintCompression
from .conversions import hint_encoding_from_chardet
from typing import List, IO, Optional, Iterator, Dict
from records_mover.utils.rewound_fileobj import rewound_fileobj
from records_mover.mover_types import _assert_never
import logging


logger = logging.getLogger(__name__)

HINT_INFERENCE_SAMPLING_SIZE_BYTES = 1024


@contextmanager
def rewound_decompressed_fileobj(fileobj: IO[bytes],
                                 compression: HintCompression) -> Iterator[IO[bytes]]:
    with rewound_fileobj(fileobj) as fileobj_after_rewind:
        if compression is None:
            yield fileobj
        elif compression == 'GZIP':
            yield gzip.GzipFile(mode='rb', fileobj=fileobj_after_rewind)  # type: ignore
        elif compression == 'LZO':
            # This might be useful to implement this:
            #  https://github.com/ir193/python-lzo/blob/master/lzo.py#L44
            raise NotImplementedError('Records mover does not currently know how '
                                      'to decompress LZO files for inspection')
        elif compression == 'BZIP':
            yield bz2.BZ2File(mode='rb', filename=fileobj_after_rewind)
        else:
            _assert_never(compression)


def infer_newline_format(fileobj: IO[bytes],
                         encoding_hint: HintEncoding,
                         compression: HintCompression) ->\
        Optional[HintRecordTerminator]:
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


def sniff_encoding_hint(fileobj: IO[bytes]) -> Optional[HintEncoding]:
    with rewound_fileobj(fileobj) as fileobj:
        detector = chardet.UniversalDetector()
        while True:
            chunk = fileobj.read(HINT_INFERENCE_SAMPLING_SIZE_BYTES)
            detector.feed(chunk)
            if detector.done or len(chunk) < HINT_INFERENCE_SAMPLING_SIZE_BYTES:
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


def csv_hints_from_python(fileobj: IO[bytes],
                          record_terminator_hint: Optional[HintRecordTerminator],
                          encoding_hint: HintEncoding,
                          compression: HintCompression) -> PartialRecordsHints:
    # https://docs.python.org/3/library/csv.html#csv.Sniffer
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
            sniffer = csv.Sniffer()
            sample = text_fileobj.read(HINT_INFERENCE_SAMPLING_SIZE_BYTES)
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
            out: PartialRecordsHints = {
                'doublequote': True if dialect.doublequote else False,
                'field-delimiter': dialect.delimiter,
                'header-row': True if header_row else False,
            }
            if dialect.quotechar is not None:
                out['quotechar'] = dialect.quotechar
            logger.info(f"Python csv.Dialect sniffed: {out}")
            return out
        except csv.Error as e:
            if str(e) == 'Could not determine delimiter':
                logger.info(f"Error from csv.Sniffer--potential single-field file: {str(e)}")
                return {}
            else:
                logger.info(f"Error from csv.Sniffer--potential single-field file: {str(e)}")
                raise
        finally:
            text_fileobj.detach()


def csv_hints_from_pandas(fileobj: IO[bytes],
                          streaming_hints: PartialRecordsHints) -> PartialRecordsHints:
    import pandas

    def attempt_parse(quoting: HintQuoting) -> PartialRecordsHints:
        with rewound_fileobj(fileobj) as fresh_fileobj:
            current_hints = streaming_hints.copy()
            current_hints['quoting'] = quoting
            logger.info(f"Attempting to parse with quoting: {quoting}")
            with stream_csv(fresh_fileobj, current_hints):
                return {
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


def sniff_hints_from_fileobjs(fileobjs: List[IO[bytes]],
                              initial_hints: PartialRecordsHints) -> PartialRecordsHints:
    if len(fileobjs) != 1:
        # https://github.com/bluelabsio/records-mover/issues/84
        raise NotImplementedError('Cannot currently sniff hints from multiple '
                                  'files--please provide hints')
    fileobj = fileobjs[0]
    hints = sniff_hints(fileobj, initial_hints=initial_hints)
    return hints


def sniff_hints(fileobj: IO[bytes],
                initial_hints: PartialRecordsHints) -> PartialRecordsHints:
    # Major limitations:
    #
    #  * If fileobj isn't rewindable, we can't sniff or we'd keep you
    #    from being able to use later.
    #  * We can't sniff from LZO files yet.
    #  * No detection of 'escape' or date/time format hints.
    #  * Only limited detection of 'quoting' hint.
    try:
        #
        # We'll need to determine compression and encoding to be able
        # to convert from bytes to characters and figure out the rest:
        #
        if 'compression' not in initial_hints:
            compression_hint = sniff_compression_hint(fileobj)
        else:
            compression_hint = initial_hints['compression']
        if 'encoding' not in initial_hints:
            encoding_hint = sniff_encoding_hint(fileobj)
        else:
            encoding_hint = initial_hints['encoding']
        # If guessing was inconclusive, default to UTF8
        final_encoding_hint: HintEncoding = (encoding_hint or 'UTF8')

        #
        # Now we can figure out what type of newlines are used in the
        # file...
        #
        record_terminator_hint: Optional[HintRecordTerminator] = None
        if 'record-terminator' in initial_hints:
            record_terminator_hint = initial_hints['record-terminator']
        else:
            record_terminator_hint = infer_newline_format(fileobj,
                                                          final_encoding_hint,
                                                          compression_hint)

        #
        # Now we have enough to study each line of the file, Python's
        # csv.Sniffer() can teach us some things about how each field
        # is encoded and whether there's a header:
        #
        other_inferred_csv_hints = {}
        if record_terminator_hint is not None:
            other_inferred_csv_hints['record-terminator'] = record_terminator_hint
            python_inferred_hints = csv_hints_from_python(fileobj,
                                                          record_terminator_hint,
                                                          final_encoding_hint,
                                                          compression_hint)
        else:
            python_inferred_hints = {}

        #
        # Pandas can both validate that we chose correctly by parsing
        # the file using what we have so far, and give us a crude shot
        # at finding a working 'quoting' hint:
        #
        streaming_hints = initial_hints.copy()
        streaming_hints['compression'] = compression_hint
        if encoding_hint is not None:
            streaming_hints['encoding'] = encoding_hint
        streaming_hints.update(python_inferred_hints)
        pandas_inferred_hints = csv_hints_from_pandas(fileobj, streaming_hints)

        #
        # Let's combine these together and present back a refined
        # version of the initial hints:
        #
        out: PartialRecordsHints = {
            'compression': compression_hint,
            **pandas_inferred_hints,  # type: ignore
            **python_inferred_hints,
            'encoding': final_encoding_hint,
            **other_inferred_csv_hints,
            **initial_hints
        }
        logger.info(f"Inferred hints from combined sources: {out}")
        return out
    except OSError:
        logger.warning("Could not sniff hints, as stream is not rewindable")
        return {}
    except NotImplementedError as e:
        logger.warning(f"Could not sniff hints due to current limitations in records mover: {e}")
        return {}
