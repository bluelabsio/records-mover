from records_mover.utils import quiet_remove
from records_mover.records.delimited import cant_handle_hint, ValidatedRecordsHints
from typing import Optional, Set
from .types import DateOrderStyle


def determine_input_date_order_style(unhandled_hints: Set[str],
                                     hints: ValidatedRecordsHints,
                                     fail_if_cant_handle_hint: bool) ->\
        Optional[DateOrderStyle]:

    # https://www.postgresql.org/docs/9.5/datatype-datetime.html#DATATYPE-DATETIME-INPUT

    # "Date and time input is accepted in almost any reasonable
    # format, including ISO 8601, SQL-compatible, traditional
    # POSTGRES, and others. For some formats, ordering of day, month,
    # and year in date input is ambiguous and there is support for
    # specifying the expected ordering of these fields. Set the
    # DateStyle parameter to MDY to select month-day-year
    # interpretation, DMY to select day-month-year interpretation, or
    # YMD to select year-month-day interpretation."

    # $ cd tests/integration
    # $ ./itest shell
    # $ db dockerized-postgres
    # postgres=# SHOW DateStyle;
    #  DateStyle
    # -----------
    #  ISO, MDY
    # (1 row)
    #
    # postgres=#

    handler = DateInputStyleHandler(
        unhandled_hints, hints, fail_if_cant_handle_hint)

    return handler.date_order_style


class DateInputStyleHandler():
    def __init__(self,
                 unhandled_hints: Set[str],
                 hints: ValidatedRecordsHints,
                 fail_if_cant_handle_hint: bool):

        self.unhandled_hints = unhandled_hints
        self.hints = hints
        self.fail_if_cant_handle_hint = fail_if_cant_handle_hint
        self.date_order_style: Optional[DateOrderStyle] = None

        self.process_hint_datetimeformattz()
        self.process_hint_datetimeformat()
        self.process_hint_timeonlyformat()
        self.process_hint_dateformat()

    def upgrade_date_order_style(self, style: DateOrderStyle, hint_name: str) -> None:
        if self.date_order_style not in (None, style):
            cant_handle_hint(self.fail_if_cant_handle_hint, hint_name, self.hints)
        else:
            self.date_order_style = style
            quiet_remove(self.unhandled_hints, hint_name)

    def process_hint_datetimeformattz(self):
        datetimeformattz = self.hints.datetimeformattz

        # datetimeformattz: Valid values: "YYYY-MM-DD HH:MI:SSOF",
        # "YYYY-MM-DD HH:MI:SS", "YYYY-MM-DD HH24:MI:SSOF", "YYYY-MM-DD
        # HH24:MI:SSOF", "MM/DD/YY HH24:MI". See Redshift docs for more
        # information (note that HH: is equivalent to HH24: and that if
        # you don't provide an offset (OF), times are assumed to be in
        # UTC).

        # Default value is "YYYY-MM-DD HH:MI:SSOF".

        #
        # To get a postgres database to test these cases with:
        #
        # cd tests/integration
        # ./itest shell
        # db dockerized-postgres
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
            quiet_remove(self.unhandled_hints, 'datetimeformattz')
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
            quiet_remove(self.unhandled_hints, 'datetimeformattz')
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
            self.upgrade_date_order_style('MDY', 'datetimeformattz')
        elif datetimeformattz == 'DD-MM-YY HH:MI:SSOF':
            #  postgres=# select timestamptz '02-01-2999 23:01:01-10';
            #        timestamptz
            #  ------------------------
            #   2999-02-02 09:01:01+00
            #  (1 row)
            #
            self.upgrade_date_order_style('DMY', 'datetimeformattz')
        elif datetimeformattz == 'DD/MM/YY HH:MI:SSOF':
            #  postgres=# select timestamptz '02/01/99 23:01:01-10';
            #        timestamptz
            #  ------------------------
            #   1999-02-02 09:01:01+00
            #  (1 row)
            #
            self.upgrade_date_order_style('DMY', 'datetimeformattz')
        elif datetimeformattz == 'DD-MM-YYYY HH:MI:SSOF':
            #  postgres=# select timestamptz '02-01-1999 23:01:01-10';
            #        timestamptz
            #  ------------------------
            #   1999-02-02 09:01:01+00
            #  (1 row)
            #
            self.upgrade_date_order_style('DMY', 'datetimeformattz')
        elif datetimeformattz == 'MM/DD/YY HH:MI:SSOF':
            #  postgres=# select timestamptz '01/02/99 23:01:01-10';
            #        timestamptz
            #  ------------------------
            #   1999-01-03 09:01:01+00
            #  (1 row)
            #
            self.upgrade_date_order_style('MDY', 'datetimeformattz')
        elif datetimeformattz == 'MM-DD-YYYY HH:MI:SSOF':
            #  postgres=# select timestamptz '01-02-1999 23:01:01-10';
            #        timestamptz
            #  ------------------------
            #   1999-01-03 09:01:01+00
            #  (1 row)
            #
            self.upgrade_date_order_style('MDY', 'datetimeformattz')
        else:
            cant_handle_hint(self.fail_if_cant_handle_hint,
                             'datetimeformattz', self.hints)

    def process_hint_datetimeformat(self):
        # datetimeformat: Valid values: "YYYY-MM-DD HH24:MI:SS",
        # "YYYY-MM-DD HH12:MI AM", "MM/DD/YY HH24:MI". See Redshift docs
        # for more information.

        datetimeformat = self.hints.datetimeformat

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
            quiet_remove(self.unhandled_hints, 'datetimeformat')
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
            quiet_remove(self.unhandled_hints, 'datetimeformat')
        elif datetimeformat == "MM/DD/YY HH24:MI":
            #  postgres=# select timestamp '01/02/20 15:23';
            #        timestamp
            #  ---------------------
            #   2020-01-02 15:23:00
            #  (1 row)
            #
            #  postgres=#

            self.upgrade_date_order_style('MDY', 'datetimeformat')
        elif datetimeformat == 'DD-MM-YY HH12:MI AM':
            #  postgres=# select timestamp '02-01-20 3:23 PM';
            #        timestamp
            #  ---------------------
            #   2020-02-01 15:23:00
            #  (1 row)
            #
            #  postgres=#
            self.upgrade_date_order_style('DMY', 'datetimeformat')
        elif datetimeformat == 'DD/MM/YY HH12:MI AM':
            #  postgres=# select timestamp '02/01/20 3:23 PM';
            #        timestamp
            #  ---------------------
            #   2020-02-01 15:23:00
            #  (1 row)
            #
            #  postgres=#
            self.upgrade_date_order_style('DMY', 'datetimeformat')
        elif datetimeformat == 'DD-MM-YYYY HH12:MI AM':
            #  postgres=# select timestamp '02-01-2020 3:23 PM';
            #        timestamp
            #  ---------------------
            #   2020-02-01 15:23:00
            #  (1 row)
            #
            #  postgres=#
            self.upgrade_date_order_style('DMY', 'datetimeformat')
        elif datetimeformat == 'MM-DD-YY HH12:MI AM':
            #  postgres=# select timestamp '02-01-20 3:23 PM';
            #        timestamp
            #  ---------------------
            #   2020-02-01 15:23:00
            #  (1 row)
            #
            #  postgres=#
            self.upgrade_date_order_style('MDY', 'datetimeformat')
        elif datetimeformat == 'MM/DD/YY HH12:MI AM':
            #  postgres=# select timestamp '02/01/20 3:23 PM';
            #        timestamp
            #  ---------------------
            #   2020-02-01 15:23:00
            #  (1 row)
            #
            #  postgres=#
            self.upgrade_date_order_style('MDY', 'datetimeformat')
        elif datetimeformat == 'MM-DD-YYYY HH12:MI AM':
            #  postgres=# select timestamp '01-02-2020 3:23 PM';
            #        timestamp
            #  ---------------------
            #   2020-01-02 15:23:00
            #  (1 row)
            #
            #  postgres=#
            self.upgrade_date_order_style('MDY', 'datetimeformat')
        elif datetimeformat == 'MM-DD-YYYY HH:MI:SS':
            #  postgres=# select timestamp '01-02-2020 3:23 PM';
            #        timestamp
            #  ---------------------
            #   2020-01-02 15:23:00
            #  (1 row)
            #
            #  postgres=#
            self.upgrade_date_order_style('MDY', 'datetimeformat')
        else:
            cant_handle_hint(self.fail_if_cant_handle_hint,
                             'datetimeformat', self.hints)

    def process_hint_timeonlyformat(self):
        timeonlyformat = self.hints.timeonlyformat

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
            quiet_remove(self.unhandled_hints, 'timeonlyformat')
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
            quiet_remove(self.unhandled_hints, 'timeonlyformat')
        elif timeonlyformat == "HH:MI:SS":
            #  postgres=# select time '13:00:00';
            #     time
            #  ----------
            #   13:00:00
            #  (1 row)
            #
            #  postgres=#
            quiet_remove(self.unhandled_hints, 'timeonlyformat')
        elif timeonlyformat == "HH24:MI":

            # "HH24:MI" (e.g., "13:00")
            #
            #  postgres=# select time '13:00';
            #     time
            #  ----------
            #   13:00:00
            #  (1 row)
            #
            #  postgres=#

            # Supported!
            quiet_remove(self.unhandled_hints, 'timeonlyformat')
        else:
            cant_handle_hint(self.fail_if_cant_handle_hint, 'timeonlyformat', self.hints)

    def process_hint_dateformat(self):
        # dateformat: Valid values:
        # null, "YYYY-MM-DD", "MM-DD-YYYY", "DD-MM-YYYY", "MM/DD/YY".
        dateformat = self.hints.dateformat

        if dateformat == "YYYY-MM-DD":
            #  postgres=# select date '1999-01-02';
            #      date
            #  ------------
            #   1999-01-02
            #  (1 row)
            #
            #  postgres=#
            # Any DateStyle will do as this is unambiguous
            quiet_remove(self.unhandled_hints, 'dateformat')
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
            self.upgrade_date_order_style('MDY', 'dateformat')
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
            self.upgrade_date_order_style('DMY', 'dateformat')
        elif dateformat == "DD-MM-YY":
            #  postgres=# select date '02-01-99';
            #      date
            #  ------------
            #   1999-02-01
            #  (1 row)
            #
            #  postgres=#
            self.upgrade_date_order_style('DMY', 'dateformat')
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
            self.upgrade_date_order_style('MDY', 'dateformat')
        elif dateformat == "DD/MM/YY":
            #  postgres=# select date '02/01/99';
            #      date
            #  ------------
            #   1999-02-01
            #  (1 row)
            #
            #  postgres=#
            self.upgrade_date_order_style('DMY', 'dateformat')
        elif dateformat is None:
            # null implies that that date format is unknown, and that the
            # implementation SHOULD generate using their default value and
            # parse permissively.

            # ...which is what Postgres does!
            quiet_remove(self.unhandled_hints, 'dateformat')
        else:
            cant_handle_hint(self.fail_if_cant_handle_hint,
                             'dateformat', self.hints)
