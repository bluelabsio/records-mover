from records_mover.utils import quiet_remove
from records_mover.records.hints import cant_handle_hint
from records_mover.records.types import RecordsHints
from typing import Optional, Set, Union, TYPE_CHECKING
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
    DateInputStyle = Union[Literal["DMY"], Literal["MDY"]]
else:
    DateInputStyle = str


def determine_date_input_style(unhandled_hints: Set[str],
                               hints: RecordsHints,
                               fail_if_cant_handle_hint: bool) ->\
        Optional[DateInputStyle]:
    date_input_style: Optional[DateInputStyle] = None

    def upgrade_date_input_style(style: DateInputStyle, hint_name: str) -> None:
        nonlocal date_input_style
        if date_input_style not in (None, style):
            cant_handle_hint(fail_if_cant_handle_hint, hint_name, hints)
        else:
            date_input_style = style
            quiet_remove(unhandled_hints, hint_name)

    # https://www.postgresql.org/docs/9.5/datatype-datetime.html#DATATYPE-DATETIME-INPUT

    # "Date and time input is accepted in almost any reasonable
    # format, including ISO 8601, SQL-compatible, traditional
    # POSTGRES, and others. For some formats, ordering of day, month,
    # and year in date input is ambiguous and there is support for
    # specifying the expected ordering of these fields. Set the
    # DateStyle parameter to MDY to select month-day-year
    # interpretation, DMY to select day-month-year interpretation, or
    # YMD to select year-month-day interpretation."

    # $ ./itest shell
    # $ db dockerized-postgres
    # postgres=# SHOW DateStyle;
    #  DateStyle
    # -----------
    #  ISO, MDY
    # (1 row)
    #
    # postgres=#

    datetimeformattz = hints['datetimeformattz']

    # datetimeformattz: Valid values: "YYYY-MM-DD HH:MI:SSOF",
    # "YYYY-MM-DD HH:MI:SS", "YYYY-MM-DD HH24:MI:SSOF", "YYYY-MM-DD
    # HH24:MI:SSOF", "MM/DD/YY HH24:MI". See Redshift docs for more
    # information (note that HH: is equivalent to HH24: and that if
    # you don't provide an offset (OF), times are assumed to be in
    # UTC).

    # Default value is "YYYY-MM-DD HH:MI:SSOF".

    if datetimeformattz in ['YYYY-MM-DD HH:MI:SSOF',
                            'YYYY-MM-DD HH24:MI:SSOF']:
        #
        #  postgres=# select timestamptz '2020-01-01 23:01:01-10';
        #        timestamptz
        #  ------------------------
        #   2020-01-02 09:01:01+00
        #  (1 row)
        #
        #  postgres=#

        # Any DateStyle will do as this is unambiguous
        quiet_remove(unhandled_hints, 'datetimeformattz')
    elif datetimeformattz == 'YYYY-MM-DD HH:MI:SS':
        #
        #
        #  postgres=# select timestamptz '2020-01-01 23:01:01';
        #        timestamptz
        #  ------------------------
        #   2020-01-01 23:01:01+00
        #  (1 row)
        #
        #  postgres=#

        # Any DateStyle will do as this is unambiguous
        quiet_remove(unhandled_hints, 'datetimeformattz')
    elif datetimeformattz == "MM/DD/YY HH24:MI":
        # "MM/DD/YY HH24:MI"
        #
        #  postgres=# select timestamptz '01/02/2999 23:01';
        #        timestamptz
        #  ------------------------
        #   2999-01-02 23:01:00+00
        #  (1 row)
        #
        #  postgres=#
        upgrade_date_input_style('MDY', 'datetimeformattz')
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformattz', hints)

    # datetimeformat: Valid values: "YYYY-MM-DD HH24:MI:SS",
    # "YYYY-MM-DD HH12:MI AM", "MM/DD/YY HH24:MI". See Redshift docs
    # for more information.

    datetimeformat = hints['datetimeformat']

    if datetimeformat in ("YYYY-MM-DD HH24:MI:SS",
                          "YYYY-MM-DD HH:MI:SS"):
        #
        #  postgres=# select timestamp '2020-01-02 15:13:12';
        #        timestamp
        #  ---------------------
        #   2020-01-02 15:13:12
        #  (1 row)
        #
        #  postgres=#

        # Any DateStyle will do as this is unambiguous
        quiet_remove(unhandled_hints, 'datetimeformat')
    elif datetimeformat == "YYYY-MM-DD HH12:MI AM":
        # "YYYY-MM-DD HH12:MI AM"
        #
        #  postgres=# select timestamp '2020-01-02 1:13 PM';
        #        timestamp
        #  ---------------------
        #   2020-01-02 13:13:00
        #  (1 row)
        #
        #  postgres=#

        # Any DateStyle will do as this is unambiguous
        quiet_remove(unhandled_hints, 'datetimeformat')
    elif datetimeformat == "MM/DD/YY HH24:MI":
        #  postgres=# select timestamp '01/02/20 15:23';
        #        timestamp
        #  ---------------------
        #   2020-01-02 15:23:00
        #  (1 row)
        #
        #  postgres=#

        upgrade_date_input_style('MDY', 'datetimeformat')
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformat', hints)

    timeonlyformat = hints['timeonlyformat']

    # timeonlyformat: Valid values: "HH12:MI AM" (e.g., "1:00 PM"),
    # "HH24:MI:SS" (e.g., "13:00:00")

    if timeonlyformat == "HH12:MI AM":
        # "HH12:MI AM" (e.g., "1:00 PM"),
        #
        #  postgres=# select time '1:00 PM';
        #     time
        #  ----------
        #   13:00:00
        #  (1 row)
        #
        #  postgres=#

        # Supported!
        quiet_remove(unhandled_hints, 'timeonlyformat')
    elif timeonlyformat == "HH24:MI:SS":

        # "HH24:MI:SS" (e.g., "13:00:00")
        #
        #  postgres=# select time '13:00:00';
        #     time
        #  ----------
        #   13:00:00
        #  (1 row)
        #
        #  postgres=#

        # Supported!
        quiet_remove(unhandled_hints, 'timeonlyformat')
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformat', hints)

    # dateformat: Valid values: null, "YYYY-MM-DD", "MM-DD-YYYY", "DD-MM-YYYY", "MM/DD/YY".
    dateformat = hints['dateformat']

    if dateformat == "YYYY-MM-DD":
        #  postgres=# select date '1999-01-02';
        #      date
        #  ------------
        #   1999-01-02
        #  (1 row)
        #
        #  postgres=#
        # Any DateStyle will do as this is unambiguous
        quiet_remove(unhandled_hints, 'dateformat')
    elif dateformat == "MM-DD-YYYY":
        # "MM-DD-YYYY"
        #
        #  postgres=# select date '01-02-1999';
        #      date
        #  ------------
        #   1999-01-02
        #  (1 row)
        #
        #  postgres=#
        upgrade_date_input_style('MDY', 'dateformat')
    elif dateformat == "DD-MM-YYYY":
        # "DD-MM-YYYY" - not supported by default, need to switch to
        # DMY:
        #
        #  postgres=# select date '02-01-1999';
        #      date
        #  ------------
        #   1999-02-01
        #  (1 row)
        #
        #  postgres=#
        upgrade_date_input_style('DMY', 'dateformat')
    elif dateformat == "MM/DD/YY":
        # "MM/DD/YY".
        #
        #  postgres=# select date '01/02/99';
        #      date
        #  ------------
        #   1999-01-02
        #  (1 row)
        #
        #  postgres=#
        upgrade_date_input_style('MDY', 'dateformat')
    elif dateformat is None:
        # null implies that that date format is unknown, and that the
        # implementation SHOULD generate using their default value and
        # parse permissively.

        # ...which is what Postgres does!
        quiet_remove(unhandled_hints, 'dateformat')
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'dateformat', hints)

    return date_input_style
