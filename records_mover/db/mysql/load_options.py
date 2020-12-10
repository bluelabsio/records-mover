from sqlalchemy.sql.expression import text, TextClause
from records_mover.utils import quiet_remove
from records_mover.records.delimited import cant_handle_hint
from records_mover.records import DelimitedRecordsFormat
from records_mover.records.delimited import (
    HintEncoding, HintRecordTerminator,
    HintFieldDelimiter, HintQuoteChar,
    HintQuoting, HintEscape,
    HintHeaderRow, HintCompression,
    HintDoublequote,
)
from typing import Optional, Set, Dict, NamedTuple, NoReturn, TYPE_CHECKING
if TYPE_CHECKING:
    from typing_extensions import Literal
    # http://dev.mysql.com/doc/refman/8.0/en/charset-mysql.html
    MySqlCharacterSet = Literal['big5', 'binary', 'latin1', 'ucs2',
                                'utf8', 'utf8mb4', 'utf16', 'utf16le',
                                'utf32']
else:
    MySqlCharacterSet = str


# The NoReturn annotations are the mypy way of validating we're
# covering all officially approved cases of an the hint at
# typechecking time.
#
# https://github.com/python/mypy/issues/6366#issuecomment-560369716
def _assert_never(the_bad_hint: NoReturn) -> NoReturn:
    assert False


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
class MySqlLoadOptions(NamedTuple):
    character_set: MySqlCharacterSet
    fields_terminated_by: str  # default '\t'
    fields_enclosed_by: Optional[str]  # default None
    fields_optionally_enclosed_by: Optional[str]  # default None
    fields_escaped_by: Optional[str]  # default '\'
    lines_starting_by: str  # default ''
    lines_terminated_by: str  # default '\n'
    ignore_n_lines: int

    def generate_load_data_sql(self,
                               filename: str,
                               schema_name: str,
                               table_name: str) -> TextClause:
        sql = f"""\
LOAD DATA
LOCAL INFILE :filename
INTO TABLE {schema_name}.{table_name}
CHARACTER SET :character_set
FIELDS
    TERMINATED BY :fields_terminated_by
"""
        if self.fields_enclosed_by is not None and self.fields_optionally_enclosed_by is None:
            sql += "    ENCLOSED BY :fields_enclosed_by\n"
        elif self.fields_enclosed_by is None and self.fields_optionally_enclosed_by is not None:
            sql += "    OPTIONALLY ENCLOSED BY :fields_optionally_enclosed_by\n"
        elif self.fields_enclosed_by is not None and self.fields_optionally_enclosed_by is not None:
            raise SyntaxError('fields_enclosed_by and optionally_fields_enclosed_by '
                              'cannot both be set')
        if self.fields_escaped_by is not None:
            sql += "    ESCAPED BY :fields_escaped_by\n"
        sql += """\
LINES
    STARTING BY :lines_starting_by
    TERMINATED BY :lines_terminated_by
IGNORE :ignore_n_lines LINES
"""
        clause = text(sql)
        clause = clause.\
            bindparams(filename=filename,
                       character_set=self.character_set,
                       fields_terminated_by=self.fields_terminated_by,
                       lines_starting_by=self.lines_starting_by,
                       lines_terminated_by=self.lines_terminated_by,
                       ignore_n_lines=self.ignore_n_lines)
        if self.fields_enclosed_by is not None:
            clause = clause.bindparams(fields_enclosed_by=self.
                                       fields_enclosed_by)
        if self.fields_optionally_enclosed_by is not None:
            clause = clause.bindparams(fields_optionally_enclosed_by=self.
                                       fields_optionally_enclosed_by)
        if self.fields_escaped_by is not None:
            clause = clause.bindparams(fields_escaped_by=self.
                                       fields_escaped_by)
        return clause


def mysql_load_options(unhandled_hints: Set[str],
                       records_format: DelimitedRecordsFormat,
                       fail_if_cant_handle_hint: bool) -> MySqlLoadOptions:
    hints = records_format.validate(fail_if_cant_handle_hint=fail_if_cant_handle_hint)

    #
    # The server uses the character set indicated by the
    # character_set_database system variable to interpret the
    # information in the file. SET NAMES and the setting of
    # character_set_client do not affect interpretation of input. If
    # the contents of the input file use a character set that differs
    # from the default, it is usually preferable to specify the
    # character set of the file by using the CHARACTER SET clause. A
    # character set of binary specifies “no conversion.”
    #
    hint_encoding: HintEncoding = hints.encoding
    character_set = MYSQL_CHARACTER_SETS_FOR_LOAD.get(hint_encoding)
    if character_set is not None:
        mysql_character_set = character_set
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'encoding', records_format.hints)
        mysql_character_set = 'utf8'
    quiet_remove(unhandled_hints, 'encoding')

    field_terminator: HintFieldDelimiter = hints.field_delimiter
    mysql_fields_terminator = field_terminator
    quiet_remove(unhandled_hints, 'field-delimiter')

    # https://dev.mysql.com/doc/refman/8.0/en/load-data.html
    #
    #
    # LOAD DATA can be used to read files obtained from external
    # sources. For example, many programs can export data in
    # comma-separated values (CSV) format, such that lines have fields
    # separated by commas and enclosed within double quotation marks,
    # with an initial line of column names. If the lines in such a
    # file are terminated by carriage return/newline pairs, the
    # statement shown here illustrates the field- and line-handling
    # options you would use to load the file:
    #
    # LOAD DATA INFILE 'data.txt' INTO TABLE tbl_name
    #  FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    #  LINES TERMINATED BY '\r\n'
    #  IGNORE 1 LINES;
    #
    #
    mysql_fields_enclosed_by = None
    mysql_fields_optionally_enclosed_by = None
    hint_quotechar: HintQuoteChar = hints.quotechar
    hint_quoting: HintQuoting = hints.quoting
    if hint_quoting == 'all':
        mysql_fields_enclosed_by = hint_quotechar
    elif hint_quoting == 'minimal':
        # "If the input values are not necessarily enclosed within
        # quotation marks, use OPTIONALLY before the ENCLOSED BY option."
        #
        # This implies to me that parsing here is permissive -
        # otherwise unambiguous strings without double quotes around
        # them will be understood as a string, not rejected.
        mysql_fields_optionally_enclosed_by = hint_quotechar
    elif hint_quoting == 'nonnumeric':
        mysql_fields_optionally_enclosed_by = hint_quotechar
    elif hint_quoting is None:
        pass
    else:
        _assert_never(hint_quotechar)
    quiet_remove(unhandled_hints, 'quoting')
    quiet_remove(unhandled_hints, 'quotechar')

    # If the field begins with the ENCLOSED BY character, instances of
    # that character are recognized as terminating a field value only
    # if followed by the field or line TERMINATED BY sequence. To
    # avoid ambiguity, occurrences of the ENCLOSED BY character within
    # a field value can be doubled and are interpreted as a single
    # instance of the character. For example, if ENCLOSED BY '"' is
    # specified, quotation marks are handled as shown here:
    hint_doublequote: HintDoublequote = hints.doublequote
    if hint_quoting is not None:
        # We need to ignore flake8's "is vs ==" check because 'is'
        # doesn't work currently with MyPy's Literal[] case checking
        if hint_doublequote == True:  # noqa: E712
            pass
        elif hint_doublequote == False:  # noqa: E712
            cant_handle_hint(fail_if_cant_handle_hint, 'doublequote', records_format.hints)
        else:
            _assert_never(hint_doublequote)
    quiet_remove(unhandled_hints, 'doublequote')

    # FIELDS ESCAPED BY controls how to read or write special characters:
    #
    # * For input, if the FIELDS ESCAPED BY character is not empty,
    #   occurrences of that character are stripped and the following
    #   character is taken literally as part of a field value. Some
    #   two-character sequences that are exceptions, where the first
    #   character is the escape character.
    #
    # [...]
    #
    # If the FIELDS ESCAPED BY character is empty, escape-sequence
    # interpretation does not occur.
    hint_escape: HintEscape = hints.escape
    if hint_escape is None:
        mysql_fields_escaped_by = None
    elif hint_escape == '\\':
        mysql_fields_escaped_by = '\\'
    else:
        _assert_never(hint_quoting)
    quiet_remove(unhandled_hints, 'escape')

    mysql_lines_starting_by = ''

    hint_record_terminator: HintRecordTerminator = hints.record_terminator
    mysql_lines_terminated_by = hint_record_terminator
    quiet_remove(unhandled_hints, 'record-terminator')

    hint_header_row: HintHeaderRow = hints.header_row
    # We need to ignore flake8's "is vs ==" check because 'is'
    # doesn't work currently with MyPy's Literal[] case checking
    if hint_header_row == True:  # noqa: E712
        mysql_ignore_n_lines = 1
    elif hint_header_row == False:  # noqa: E712
        mysql_ignore_n_lines = 0
    else:
        _assert_never(hint_header_row)
    quiet_remove(unhandled_hints, 'header-row')

    hint_compression: HintCompression = hints.compression
    if hint_compression is not None:
        cant_handle_hint(fail_if_cant_handle_hint, 'compression', records_format.hints)
    quiet_remove(unhandled_hints, 'compression')

    #
    # Testing of date/time parsing in MySQL has shown it to be pretty
    # conservative.
    #
    # That said, for DD/MM and MM/DD support, we may want to look into
    # "set trade_date" per
    # https://stackoverflow.com/questions/44171283/load-data-local-infile-with-sqlalchemy-and-pymysql
    #
    if 'YYYY-MM-DD' not in hints.datetimeformat or 'AM' in hints.datetimeformat:
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformat', records_format.hints)
    quiet_remove(unhandled_hints, 'datetimeformat')
    if 'YYYY-MM-DD' not in hints.datetimeformattz or 'AM' in hints.datetimeformattz:
        cant_handle_hint(fail_if_cant_handle_hint, 'datetimeformat', records_format.hints)
    quiet_remove(unhandled_hints, 'datetimeformattz')
    if hints.dateformat != 'YYYY-MM-DD':
        cant_handle_hint(fail_if_cant_handle_hint, 'dateformat', records_format.hints)
    quiet_remove(unhandled_hints, 'dateformat')
    if 'AM' in hints.timeonlyformat:
        cant_handle_hint(fail_if_cant_handle_hint, 'timeonlyformat', records_format.hints)
    quiet_remove(unhandled_hints, 'timeonlyformat')

    return MySqlLoadOptions(character_set=mysql_character_set,
                            fields_terminated_by=mysql_fields_terminator,
                            fields_enclosed_by=mysql_fields_enclosed_by,
                            fields_optionally_enclosed_by=mysql_fields_optionally_enclosed_by,
                            fields_escaped_by=mysql_fields_escaped_by,
                            lines_starting_by=mysql_lines_starting_by,
                            lines_terminated_by=mysql_lines_terminated_by,
                            ignore_n_lines=mysql_ignore_n_lines)
