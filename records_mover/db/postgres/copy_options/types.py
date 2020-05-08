from typing import Dict, Union, NoReturn, TYPE_CHECKING
from .mode import CopyOptionsMode
if TYPE_CHECKING:
    from typing_extensions import Literal
    # https://www.postgresql.org/docs/9.5/runtime-config-client.html#GUC-DATESTYLE
    #
    # DateStyle (string)
    #
    #  Sets the display format for date and time values, as well as
    #  the rules for interpreting ambiguous date input values. For
    #  historical reasons, this variable contains two independent
    #  components: the output format specification (ISO, Postgres,
    #  SQL, or German) and the input/output specification for
    #  year/month/day ordering (DMY, MDY, or YMD). These can be set
    #  separately or together. The keywords Euro and European are
    #  synonyms for DMY; the keywords US, NonEuro, and NonEuropean are
    #  synonyms for MDY. See Section 8.5 for more information. The
    #  built-in default is ISO, MDY, but initdb will initialize the
    #  configuration file with a setting that corresponds to the
    #  behavior of the chosen lc_time locale.
    DateOrderStyle = Union[Literal["DMY"], Literal["MDY"]]

    # https://www.postgresql.org/docs/9.5/datatype-datetime.html#DATATYPE-DATETIME-OUTPUT
    #
    # The output format of the date/time types can be set to one of
    # the four styles ISO 8601, SQL (Ingres), traditional POSTGRES
    # (Unix date format), or German. The default is the ISO
    # format. (The SQL standard requires the use of the ISO 8601
    # format. The name of the "SQL" output format is a historical
    # accident.) Table 8-14 shows examples of each output style. The
    # output of the date and time types is generally only the date or
    # time part in accordance with the given examples. However, the
    # POSTGRES style outputs date-only values in ISO format.

    # Style Specification      Description                Example
    # ISO                      ISO 8601, SQL standard     1997-12-17 07:37:16-08
    # SQL                      traditional style          12/17/1997 07:37:16.00 PST
    # Postgres                 original style             Wed Dec 17 07:37:16 1997 PST
    # German                   regional style             17.12.1997 07:37:16.00 PST

    # Note: ISO 8601 specifies the use of uppercase letter T to
    # separate the date and time. PostgreSQL accepts that format on
    # input, but on output it uses a space rather than T, as shown
    # above. This is for readability and for consistency with RFC 3339
    # as well as some other database systems.

    # In the SQL and POSTGRES styles, day appears before month if DMY
    # field ordering has been specified, otherwise month appears
    # before day. (See Section 8.5.1 for how this setting also affects
    # interpretation of input values.) Table 8-15 shows examples.

    # datestyle Setting          Input Ordering     Example Output
    # SQL, DMY                   day/month/year     17/12/1997 15:37:16.00 CET
    # SQL, MDY                   month/day/year     12/17/1997 07:37:16.00 PST
    # Postgres, DMY              day/month/year     Wed 17 Dec 07:37:16 1997 PST

    # The date/time style can be selected by the user using the SET
    # datestyle command, the DateStyle parameter in the
    # postgresql.conf configuration file, or the PGDATESTYLE
    # environment variable on the server or client.

    # The formatting function to_char (see Section 9.8) is also
    # available as a more flexible way to format date/time output.
    DateOutputStyle = Literal["ISO", "SQL", "Postgres", "German"]

    CopyOptionsModeType = Union[Literal[CopyOptionsMode.LOADING],
                                Literal[CopyOptionsMode.UNLOADING]]
else:
    DateOrderStyle = str
    DateOutputStyle = str
    CopyOptionsModeType = str


PostgresCopyOptions = Dict[str, object]


# mypy way of validating we're covering all cases of an enum
#
# https://github.com/python/mypy/issues/6366#issuecomment-560369716
def _assert_never(x: NoReturn) -> NoReturn:
    assert False, "Unhandled type: {}".format(type(x).__name__)
