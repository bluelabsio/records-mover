from records_mover.utils import quiet_remove
from records_mover.records.hints import cant_handle_hint
from typing import TypedDict, Optional, Set, Dict, Any, Literal
from records_mover.records import DelimitedRecordsFormat
from records_mover.records.types import HintEncoding

# httpsutf://dev.mysql.com/doc/refman/8.0/en/charset-mysql.html
MySqlCharacterSet = Literal['big5', 'binary', 'latin1', 'ucs2',
                            'utf8', 'utf8mb4', 'utf16', 'utf16le',
                            'utf32']

MYSQL_CHARACTER_SETS_FOR_LOAD: Dict[HintEncoding, MySqlCharacterSet] = {
    "UTF8": 'utf8',
    "UTF16": 'utf16',
    "UTF16LE": 'utf16le',
    # https://dev.mysql.com/doc/refman/8.0/en/charset-unicode.html
    #
    # The MySQL implementation of UCS-2, UTF-16, and UTF-32 stores
    # characters in big-endian byte order and does not use a byte
    # order mark (BOM) at the beginning of values. Other database
    # systems might use little-endian byte order or a BOM. In such
    # cases, conversion of values will need to be performed when
    # transferring data between those systems and MySQL. The
    # implementation of UTF-16LE is little-endian.
    "UTF16BE": 'utf16',
    # MySQL uses no BOM for UTF-8 values.
    "LATIN1": 'latin1',
    #  MySQL's latin1 is the same as the Windows cp1252 character set.
    #
    # https://dev.mysql.com/doc/refman/8.0/en/charset-we-sets.html
    "CP1252": 'latin1',
}


# Mark as total=False so we can create this incrementally
class MySqlLoadOptions(TypedDict, total=False):
    character_set: MySqlCharacterSet
    field_terminator: str  # default '\t'
    field_enclosed_by: Optional[str]  # default ''
    field_optionally_enclosed_by: Optional[str]  # default None
    field_escaped_by: str  # default '\'
    line_starting_by: str  # default ''
    line_terminated_by: str  # default '\n'
    ignore_n_lines: int


def mysql_load_options(unhandled_hints: Set[str],
                       records_format: DelimitedRecordsFormat,
                       fail_if_cant_handle_hint: bool) -> MySqlLoadOptions:
    load_options: MySqlLoadOptions = {}
    hints = records_format.hints

    hint_encoding: HintEncoding = hints['encoding']  # type: ignore
    character_set = MYSQL_CHARACTER_SETS_FOR_LOAD.get(hint_encoding)
    if character_set is not None:
        load_options['character_set'] = character_set
        quiet_remove(unhandled_hints, 'encoding')
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'encoding', hints)
        load_options['character_set'] = 'utf8'

    # :  hints['record-terminator'] # HintsRecordTerminator

    return load_options
