from records_mover.records.load_plan import RecordsLoadPlan
from records_mover.records.types import RecordsHints
from records_mover.records.records_format import DelimitedRecordsFormat
import logging
from typing import Set, Tuple, Optional
from .date_input_style import DateInputStyle
from .csv import postgres_copy_options_csv
from .text import postgres_copy_options_text
from .types import PostgresCopyOptions

logger = logging.getLogger(__name__)

# https://www.postgresql.org/docs/9.2/sql-copy.html


def needs_csv_format(hints: RecordsHints) -> bool:
    # This format option is used for importing and exporting the Comma
    # Separated Value (CSV) file format used by many other programs,
    # such as spreadsheets. Instead of the escaping rules used by
    # PostgreSQL's standard text format, it produces and recognizes
    # the common CSV escaping mechanism.

    # The values in each record are separated by the DELIMITER
    # character. If the value contains the delimiter character, the
    # QUOTE character, the NULL string, a carriage return, or line
    # feed character, then the whole value is prefixed and suffixed by
    # the QUOTE character, and any occurrence within the value of a
    # QUOTE character or the ESCAPE character is preceded by the
    # escape character. You can also use FORCE_QUOTE to force quotes
    # when outputting non-NULL values in specific columns.
    if hints['header-row'] or (hints['quoting'] is not None):
        return True

    return False


def postgres_copy_options(unhandled_hints: Set[str],
                          load_plan: RecordsLoadPlan) -> Tuple[Optional[DateInputStyle],
                                                               PostgresCopyOptions]:
    fail_if_cant_handle_hint = load_plan.processing_instructions.fail_if_cant_handle_hint
    if not isinstance(load_plan.records_format, DelimitedRecordsFormat):
        raise NotImplementedError("Not currently able to import "
                                  f"{load_plan.records_format.format_type}")
    hints = load_plan.records_format.hints

    if needs_csv_format(hints):
        return postgres_copy_options_csv(unhandled_hints,
                                         hints,
                                         fail_if_cant_handle_hint)
    else:
        return postgres_copy_options_text(unhandled_hints,
                                          hints,
                                          fail_if_cant_handle_hint)
