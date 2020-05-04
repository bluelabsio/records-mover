from typing import Dict, Optional, Union, List, Mapping, NamedTuple, Any, TYPE_CHECKING
from .hints import cant_handle_hint
from records_mover.types import JsonValue

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

RecordsValue = Optional[Union[bool, str]]
RecordsHints = Mapping[str, JsonValue]
MutableRecordsHints = Dict[str, JsonValue]


if TYPE_CHECKING:
    from ..db import DBDriver  # noqa
    # https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_TABLE.html
    from mypy_extensions import TypedDict

    class RecordsManifestEntryMetadata(TypedDict):
        content_length: int

    class BaseRecordsManifestEntry(TypedDict):
        url: str
        mandatory: bool

    class LegacyRecordsManifestEntry(BaseRecordsManifestEntry, total=False):
        meta: RecordsManifestEntryMetadata

    class RecordsManifestEntryWithLength(BaseRecordsManifestEntry):
        meta: RecordsManifestEntryMetadata

    class LegacyRecordsManifest(TypedDict):
        entries: List[LegacyRecordsManifestEntry]

    class RecordsManifestWithLength(TypedDict):
        entries: List[RecordsManifestEntryWithLength]

    class UrlDetailsEntry(TypedDict):
        content_length: int

    Url = str
    UrlDetails = Dict[Url, UrlDetailsEntry]

    from typing_extensions import Literal  # noqa

    from mypy_extensions import TypedDict

    #
    # Note: Any expansion of these types should also be done in
    # records.jobs.hints
    #
    RecordsFormatType = Literal['delimited', 'parquet']

    DelimitedVariant = Literal['dumb', 'csv', 'bigquery', 'bluelabs', 'vertica']

    HintEncoding = Literal["UTF8", "UTF16", "UTF16LE", "UTF16BE",
                           "UTF16BOM", "UTF8BOM", "LATIN1", "CP1252"]

    HintQuoting = Literal["all", "minimal", "nonnumeric", None]

    HintEscape = Literal["\\", None]
else:
    RecordsManifestEntryMetadata = Mapping[str, int]
    LegacyRecordsManifestEntry = Mapping[str, Union[str, bool, int, RecordsManifestEntryMetadata]]
    RecordsManifestEntryWithLength =\
        Mapping[str, Union[str, bool, int, RecordsManifestEntryMetadata]]
    LegacyRecordsManifest = Mapping[str, List[LegacyRecordsManifestEntry]]
    RecordsManifestWithLength = Mapping[str, List[RecordsManifestEntryWithLength]]
    UrlDetailsEntry = Dict[str, int]
    Url = str
    UrlDetails = Dict[Url, UrlDetailsEntry]

    RecordsFormatType = str

    DelimitedVariant = str

    HintEncoding = str

    HintQuoting = Optional[str]

    HintEscape = Optional[str]

# TODO: combine this and cli thingie
if TYPE_CHECKING:
    HintCompression = Literal['GZIP', 'BZIP', 'LZO', None]
else:
    HintCompression = Optional[str]

VALID_COMPRESSIONS: List[HintCompression] = ['GZIP', 'BZIP', 'LZO', None]

if TYPE_CHECKING:
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
                                   "YYYY-MM-DD HH24:MI:SSOF",
                                   "YYYY-MM-DD HH24:MI:SSOF",
                                   "MM/DD/YY HH24:MI"]

    HintDateTimeFormat = Literal["YYYY-MM-DD HH24:MI:SS",
                                 "YYYY-MM-DD HH12:MI AM",
                                 "MM/DD/YY HH24:MI"]
else:
    HintHeaderRow = bool

    HintDoublequote = bool

    # TODO: This None is a bug in the spec, right?
    HintDateFormat = Optional[str]

    HintTimeOnlyFormat = str

    HintDateTimeFormatTz = str

    HintDateTimeFormat = str

HintFieldDelimiter = str

HintRecordTerminator = str

HintQuoteChar = str


if TYPE_CHECKING:
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

else:
    BootstrappingRecordsHints = RecordsHints


INVALID_OBJECT = object()


# TODO: Read up on namedtuple vs dataclass
# TODO: Read up on autovalidating libraries
class ValidatedRecordsHints(NamedTuple):
    header_row: HintHeaderRow
    field_delimiter: HintFieldDelimiter
    compression: HintCompression
    record_terminator: HintRecordTerminator
    quoting: HintQuoting
    quotechar: HintQuoteChar
    doublequote: HintDoublequote
    escape: HintEscape
    encoding: HintEncoding
    dateformat: HintDateFormat
    timeonlyformat: HintTimeOnlyFormat
    datetimeformattz: HintDateTimeFormatTz
    datetimeformat: HintDateTimeFormat

    @staticmethod
    def validate(hints: RecordsHints,
                 fail_if_cant_handle_hint: bool) -> 'ValidatedRecordsHints':
        def validate_boolean(hint_name: str) -> Union[Literal[True], Literal[False]]:
            x = hints[hint_name]
            if x is True:
                return True
            if x is False:
                return False
            cant_handle_hint(fail_if_cant_handle_hint=fail_if_cant_handle_hint,
                             hint_name=hint_name,
                             hints=hints)
            return True

        def validate_string(hint_name: str) -> str:
            x = hints[hint_name]
            if isinstance(x, str):
                return x
            cant_handle_hint(fail_if_cant_handle_hint=fail_if_cant_handle_hint,
                             hint_name=hint_name,
                             hints=hints)
            return str(x)

        def validate_optional_string(hint_name: str) -> Optional[str]:
            x = hints[hint_name]
            if x is None:
                return x
            if isinstance(x, str):
                return x
            cant_handle_hint(fail_if_cant_handle_hint=fail_if_cant_handle_hint,
                             hint_name=hint_name,
                             hints=hints)
            return str(x)

        def validate_compression() -> HintCompression:
            x = validate_optional_string('compression')
            # MyPy doesn't like looking for a generic optional string
            # in a list of specific optional strings.  It's wrong;
            # that's perfectly safe
            i = VALID_COMPRESSIONS.index(x)  # type: ignore
            if i is not None:
                return VALID_COMPRESSIONS[i]
            cant_handle_hint(fail_if_cant_handle_hint=fail_if_cant_handle_hint,
                             hint_name='compression',
                             hints=hints)
            return None

        def validate_quoting() -> HintQuoting:
            raise NotImplementedError

        header_row = validate_boolean('header_row')
        field_delimiter = validate_string('field_delimiter')
        compression = validate_compression()
        record_terminator = validate_string('record_terminator')
        quoting = validate_quoting('quoting')
        # TODO: After one create functions
        return ValidatedRecordsHints(
            header_row=header_row,
            field_delimiter=field_delimiter,
            compression=compression,
            record_terminator=record_terminator,
            quoting=quoting,
        )
