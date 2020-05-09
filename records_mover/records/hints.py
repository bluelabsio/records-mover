from typing_inspect import is_literal_type, get_args
from abc import ABCMeta, abstractmethod
import chardet
from .types import RecordsHints
from .csv_streamer import stream_csv, python_encoding_from_hint
import io
import logging
from .types import MutableRecordsHints
from typing import Iterable, List, IO, Optional, Dict, TypeVar, Generic, Type, TYPE_CHECKING
from typing_extensions import Literal, TypedDict
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
    python_encoding = python_encoding_from_hint[encoding_hint]
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


HintEncoding = Literal["UTF8", "UTF16", "UTF16LE", "UTF16BE",
                       "UTF16BOM", "UTF8BOM", "LATIN1", "CP1252"]

HintQuoting = Literal["all", "minimal", "nonnumeric", None]

HintEscape = Literal["\\", None]

# TODO: combine this and cli thingie
HintCompression = Literal['GZIP', 'BZIP', 'LZO', None]

# The trick here works on Literal[True, False] but not on bool:
#
# https://github.com/python/mypy/issues/6366#issuecomment-560369716
HintHeaderRow = Literal[True, False]

HintDoublequote = Literal[True, False]


# TODO: This None is a bug in the spec, right?
HintDateFormat = Literal[None, 'YYYY-MM-DD', 'MM-DD-YYYY', 'DD-MM-YYYY', 'MM/DD/YY']

HintTimeOnlyFormat = Literal["HH12:MI AM", "HH24:MI:SS"]

HintDateTimeFormatTz = Literal["YYYY-MM-DD HH:MI:SSOF",
                               "YYYY-MM-DD HH:MI:SS",
                               "YYYY-MM-DD HH24:MI:SSOF", # TODO: this is listed twice - bug in spec?
                               "YYYY-MM-DD HH24:MI:SSOF",
                               "MM/DD/YY HH24:MI"]

HintDateTimeFormat = Literal["YYYY-MM-DD HH24:MI:SS",
                             'YYYY-MM-DD HH:MI:SS', # TODO this isn't in spec valid, but is part of a variant
                             "YYYY-MM-DD HH12:MI AM",
                             "MM/DD/YY HH24:MI"]


HintFieldDelimiter = str

HintRecordTerminator = str

HintQuoteChar = str


BootstrappingRecordsHints = TypedDict('BootstrappingRecordsHints',
                                      {
                                          'quoting': HintQuoting,
                                          'header-row': HintHeaderRow,
                                          'field-delimiter': HintFieldDelimiter,
                                          'encoding': HintEncoding,
                                          'escape': HintEscape,
                                          'compression': HintCompression,
                                      },
                                      total=False)


# TODO: these should live in hints.py - existing stuff should probably move
HintName = Literal["header-row",
                   "field-delimiter",
                   "compression",
                   "record-terminator",
                   "quoting",
                   "quotechar",
                   "doublequote",
                   "escape",
                   "encoding",
                   "dateformat",
                   "timeonlyformat",
                   "datetimeformattz",
                   "datetimeformat"]

HintT = TypeVar('HintT')


class Hint(Generic[HintT], metaclass=ABCMeta):
    ...
    @abstractmethod
    def validate(self,
                 hints: RecordsHints,
                 fail_if_cant_handle_hint: bool) -> HintT:
        ...


class StringHint(Hint[str]):
    def __init__(self,
                 hint_name: HintName,
                 default: str) -> None:
        self.default = default
        self.hint_name = hint_name

    def validate(self,
                 hints: RecordsHints,
                 fail_if_cant_handle_hint: bool) -> str:
        x: object = hints[self.hint_name]
        if isinstance(x, str):
            return x
        else:
            cant_handle_hint(fail_if_cant_handle_hint=fail_if_cant_handle_hint,
                             hint_name=self.hint_name,
                             hints=hints)
            return self.default


LiteralHintT = TypeVar('LiteralHintT')


class LiteralHint(Hint[LiteralHintT]):
    def __init__(self,
                 type_: Type[LiteralHintT],
                 hint_name: HintName,
                 default: LiteralHintT) -> None:
        assert is_literal_type(type_), f"{hint_name} is not a Literal[]"
        self.default = default
        self.type_ = type_
        self.hint_name = hint_name
        self.valid_values: List[LiteralHintT] = list(get_args(type_))

    def validate(self,
                 hints: RecordsHints,
                 fail_if_cant_handle_hint: bool) -> LiteralHintT:
        # MyPy doesn't like looking for a generic optional string
        # in a list of specific optional strings.  It's wrong;
        # that's perfectly safe
        x: object = hints[self.hint_name]
        try:
            i = self.valid_values.index(x)  # type: ignore
            return self.valid_values[i]
        except ValueError:
            # well, sort of safe.
            cant_handle_hint(fail_if_cant_handle_hint=fail_if_cant_handle_hint,
                             hint_name=self.hint_name,
                             hints=hints)
            return self.default


class Hints:
    # mypy gives this when we pass the HintBlahBlah aliases in as an
    # argument here:
    #
    # error: The type alias to Union is invalid in runtime context
    #
    # Nonetheless, the validation works.
    datetimeformattz = LiteralHint[HintDateTimeFormatTz](HintDateTimeFormatTz,  # type: ignore
                                                         "datetimeformattz",
                                                         "YYYY-MM-DD HH24:MI:SSOF")
    datetimeformat = LiteralHint[HintDateTimeFormat](HintDateTimeFormat,  # type: ignore
                                                     "datetimeformat",
                                                     default="YYYY-MM-DD HH24:MI:SS")
    compression = LiteralHint[HintCompression](HintCompression,  # type: ignore
                                               'compression',
                                               default=None)
    quoting = LiteralHint[HintQuoting](HintQuoting,  # type: ignore
                                       'quoting',
                                       default='minimal')
    escape = LiteralHint[HintEscape](HintEscape,  # type: ignore
                                     'escape',
                                     default='\\')
    encoding = LiteralHint[HintEncoding](HintEncoding,  # type: ignore
                                         'encoding',
                                         default='UTF8')
    dateformat = LiteralHint[HintDateFormat](HintDateFormat,  # type: ignore
                                             'dateformat',
                                             default='YYYY-MM-DD')
    timeonlyformat = LiteralHint[HintTimeOnlyFormat](HintTimeOnlyFormat,  # type: ignore
                                                     'timeonlyformat',
                                                     default="HH24:MI:SS")
    doublequote = LiteralHint[HintDoublequote](HintDoublequote,  # type: ignore
                                               'doublequote',
                                               default=False)
    header_row = LiteralHint[HintHeaderRow](HintHeaderRow,  # type: ignore
                                            'header-row',
                                            default=True)
    quotechar = StringHint('quotechar', default='"')
    record_terminator = StringHint('record-terminator', default='\n')
    field_delimiter = StringHint('field-delimiter', default=',')
