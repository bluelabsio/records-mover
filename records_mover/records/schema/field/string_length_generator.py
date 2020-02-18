from .constraints.string import RecordsSchemaFieldStringConstraints
from .statistics import RecordsSchemaFieldStringStatistics
from typing import Optional, Union, TYPE_CHECKING
if TYPE_CHECKING:
    from ....db import DBDriver  # noqa


def generate_string_length(constraints: Optional[RecordsSchemaFieldStringConstraints],
                           statistics: Optional[RecordsSchemaFieldStringStatistics],
                           driver: Optional['DBDriver']) -> int:
    len_from_constraints = string_length_from_field_details(constraints, driver)
    len_from_statistics = string_length_from_field_details(statistics, driver)

    if len_from_constraints is not None:
        # trust this first, as it may have more intent from the
        # creator of the origin representation.
        return len_from_constraints

    if len_from_statistics is not None:
        return len_from_statistics

    # safe-ish default if we can't figure out any better.
    return 256


def string_length_from_field_details(details: Optional[Union[RecordsSchemaFieldStringConstraints,
                                                             RecordsSchemaFieldStringStatistics]],
                                     driver: Optional['DBDriver']) -> Optional[int]:
    if details is not None:
        if driver is None or not driver.varchar_length_is_in_chars():
            # the target tracks varchar length in bytes

            if details.max_length_bytes is not None:
                # Note: this doesn't yet take into account
                # difference in encodings; if source encoded in
                # utf-8 and was mostly plain ASCII and destination
                # is in UTF-16 or something, you're gonna have a
                # bad time.
                return details.max_length_bytes
            elif details.max_length_chars is not None:
                # Note: also not yet taking into account
                # variations in source vs destination encoding
                utf_8_max_tax = 4
                # if every character uses a maximum number of
                # bytes, we'll need this much space...
                return details.max_length_chars * utf_8_max_tax
            else:
                pass  # use the default guess above
        else:
            # in chars
            if details.max_length_chars is not None:
                return details.max_length_chars
            elif details.max_length_bytes is not None:
                return details.max_length_bytes
            else:
                pass  # don't try to guess
    return None
