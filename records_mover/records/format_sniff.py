import os
import logging
from typing import Optional, Mapping, IO
from records_mover.records.hints import BootstrappingRecordsHints
from records_mover.records.types import RecordsFormatType
from records_mover.records.records_format import (
    BaseRecordsFormat, ParquetRecordsFormat, DelimitedRecordsFormat
)
from records_mover.records.compression import sniff_compression_from_pathname
from records_mover.records.hints import sniff_hints_from_fileobjs


logger = logging.getLogger(__name__)


# TODO: Generate a mondo unit test with lots of combinations
def sniff_records_format_type_from_pathname(pathname: str) -> Optional[RecordsFormatType]:
    splitpathname = os.path.splitext(pathname)
    if len(splitpathname) != 2:
        # TODO: What does it mean if >2?
        return None
    # TODO: What about .csv.gz
    ext = splitpathname[1]
    if ext:
        extlower = ext.lower()
        if extlower == '.parquet':
            return 'parquet'
        elif extlower == '.csv':
            return "delimited"
    return None


def sniff_records_format_from_pathname(pathname: str,
                                       default_delimited_variant: str,
                                       default_records_format_type:
                                           Optional[RecordsFormatType] = 'delimited')\
            -> BaseRecordsFormat:
    records_format_type: Optional[RecordsFormatType] = \
        sniff_records_format_type_from_pathname(pathname)
    if records_format_type is None:
        records_format_type = default_records_format_type
    records_format: BaseRecordsFormat
    if records_format_type == "parquet":
        records_format = ParquetRecordsFormat()
    elif records_format_type == "delimited":
        # if we have been given free rein on format, don't
        # surprise the user by writing a compression they're not
        # expecting.
        compression = sniff_compression_from_pathname(pathname)
        inferred_hints = {
            'compression': compression
        }
        records_format = DelimitedRecordsFormat(variant=default_delimited_variant).\
            alter_hints(inferred_hints)
    else:
        # mypy doesn't yet have a way to ensure a complete
        # match on a Union of Literals:
        #
        # https://github.com/python/mypy/issues/6366
        raise NotImplementedError("Teach me to understand "
                                  f"records_format_type {records_format_type}")

    return records_format


def sniff_records_format_from_fileobjs(initial_hints: Optional[BootstrappingRecordsHints],
                                       target_names_to_input_fileobjs: Mapping[str, IO[bytes]])\
        -> BaseRecordsFormat:
    pathnames = list(target_names_to_input_fileobjs.keys())
    assert len(pathnames) > 0
    pathname = pathnames[0]
    # 'csv' isn't the most precise variant or fastest
    # variant to read, but given it's the default for Excel
    # and Google Sheets, it's the most common on import.  So,
    # if the specific variant isn't specified by the user,
    # let's assume 'csv'.
    records_format_from_pathname =\
        sniff_records_format_from_pathname(pathname, default_delimited_variant='csv')
    if isinstance(records_format_from_pathname, DelimitedRecordsFormat):
        if initial_hints is None:
            initial_hints = {}
        logger.info(f"Determining records format with initial_hints={initial_hints}")
        inferred_hints =\
            sniff_hints_from_fileobjs(list(target_names_to_input_fileobjs.values()),
                                      initial_hints=initial_hints)
        records_format = records_format_from_pathname.alter_hints(inferred_hints)
        return records_format
    else:
        return records_format_from_pathname
