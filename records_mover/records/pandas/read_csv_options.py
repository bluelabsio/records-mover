import csv
from ...utils import quiet_remove
from ..delimited import cant_handle_hint
from ..processing_instructions import ProcessingInstructions
from ..records_format import DelimitedRecordsFormat
from records_mover.records.schema import RecordsSchema
import logging
from typing import Set, Dict, Any


logger = logging.getLogger(__name__)


def pandas_read_csv_options(records_format: DelimitedRecordsFormat,
                            records_schema: RecordsSchema,
                            unhandled_hints: Set[str],
                            processing_instructions: ProcessingInstructions) -> Dict[str, Any]:
    ...
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html#pandas.read_csv

    hints = records_format.\
        validate(fail_if_cant_handle_hint=processing_instructions.fail_if_cant_handle_hint)

    fail_if_cant_handle_hint = processing_instructions.fail_if_cant_handle_hint

    pandas_options: Dict[str, object] = {}

    #
    # filepath_or_buffer : str, path object, or file-like object
    #
    # Any valid string path is acceptable. The string could be a
    # URL. Valid URL schemes include http, ftp, s3, and file. For file
    # URLs, a host is expected. A local file could be:
    # file://localhost/path/to/table.csv.
    #
    # If you want to pass in a path object, pandas accepts either
    # pathlib.Path or py._path.local.LocalPath.
    #
    # By file-like object, we refer to objects with a read() method,
    # such as a file handler (e.g. via builtin open function) or StringIO.
    #

    #
    # sep : str, default ‘,’
    #
    # (added by caller to this function)
    #
    # Delimiter to use. If sep is None, the C engine cannot
    # automatically detect the separator, but the Python parsing
    # engine can, meaning the latter will be used and automatically
    # detect the separator by Python’s builtin sniffer tool,
    # csv.Sniffer. In addition, separators longer than 1 character and
    # different from '\s+' will be interpreted as regular expressions
    # and will also force the use of the Python parsing engine. Note
    # that regex delimiters are prone to ignoring quoted data. Regex
    # example: '\r\t'.
    #

    #
    # delimiter : str, default None
    #
    # Alias for sep.
    #
    pandas_options['delimiter'] = hints.field_delimiter
    quiet_remove(unhandled_hints, 'field-delimiter')

    #
    # header : int, list of int, default ‘infer’
    #
    # Row number(s) to use as the column names, and the start of the
    # data. Default behavior is to infer the column names: if no names
    # are passed the behavior is identical to header=0 and column
    # names are inferred from the first line of the file, if column
    # names are passed explicitly then the behavior is identical to
    # header=None. Explicitly pass header=0 to be able to replace
    # existing names. The header can be a list of integers that
    # specify row locations for a multi-index on the columns
    # e.g. [0,1,3]. Intervening rows that are not specified will be
    # skipped (e.g. 2 in this example is skipped). Note that this
    # parameter ignores commented lines and empty lines if
    # skip_blank_lines=True, so header=0 denotes the first line of
    # data rather than the first line of the file.
    #
    if hints.header_row:
        pandas_options['header'] = 0
    else:
        pandas_options['header'] = None
    quiet_remove(unhandled_hints, 'header-row')

    #
    # names : array-like, optional
    #
    # List of column names to use. If file contains no header row,
    # then you should explicitly pass header=None. Duplicates in this
    # list will cause a UserWarning to be issued.
    #

    # (this may be useful when we support reading CSVs into dataframes
    # using an existing records schema that specifies column
    # names--but we don't have that now)

    #
    # index_col : int, sequence or bool, optional
    #
    # Column to use as the row labels of the DataFrame. If a sequence
    # is given, a MultiIndex is used. If you have a malformed file
    # with delimiters at the end of each line, you might consider
    # index_col=False to force pandas to not use the first column as
    # the index (row names).
    #

    # (we don't use dataframe indexes currently; any primary key
    # information would be transferred and handled via records schema)

    #
    # usecols : list-like or callable, optional
    #
    # Return a subset of the columns. If list-like, all elements must
    # either be positional (i.e. integer indices into the document
    # columns) or strings that correspond to column names provided
    # either by the user in names or inferred from the document header
    # row(s). For example, a valid list-like usecols parameter would
    # be [0, 1, 2] or ['foo', 'bar', 'baz']. Element order is ignored,
    # so usecols=[0, 1] is the same as [1, 0]. To instantiate a
    # DataFrame from data with element order preserved use
    # pd.read_csv(data, usecols=['foo', 'bar'])[['foo', 'bar']] for
    # columns in ['foo', 'bar'] order or pd.read_csv(data,
    # usecols=['foo', 'bar'])[['bar', 'foo']] for ['bar', 'foo']
    # order.
    #
    # If callable, the callable function will be evaluated against the
    # column names, returning names where the callable function
    # evaluates to True. An example of a valid callable argument would
    # be lambda x: x.upper() in ['AAA', 'BBB', 'DDD']. Using this
    # parameter results in much faster parsing time and lower memory
    # usage.
    #

    # (no current records mover facility to downselect columns during move)

    #
    # squeeze : bool, default False
    #
    # If the parsed data only contains one column then return a Series.
    #
    # (better to keep a standard format, no matter how many columsn)
    #

    #
    # prefix : str, optional
    #
    # Prefix to add to column numbers when no header, e.g. ‘X’ for X0, X1,
    #

    #
    # Not sure this actually does anything - when loading a CSV format
    # file with an empty final column name - e.g.,
    # tests/integration/resources/delimited-csv-with-header.csv - the
    # column still comes out as 'unnamed: 11'ead as 'untitled_11'.
    #
    # Leaving this in case a future version of Pandas behaves
    # better.
    #
    if pandas_options['header'] is None:
        # Pandas only accepts the prefix argument when the
        # header is marked as missing.
        #
        # https://github.com/pandas-dev/pandas/issues/27394
        # https://github.com/pandas-dev/pandas/pull/31383
        pandas_options['prefix'] = 'untitled_'

    #
    # mangle_dupe_cols : bool, default True
    #
    # Duplicate columns will be specified as ‘X’, ‘X.1’, …’X.N’,
    # rather than ‘X’…’X’. Passing in False will cause data to be
    # overwritten if there are duplicate names in the columns.
    #

    # (default is smart to avoid confusing SQL databases, which need
    # unique common names)

    #
    # dtype : Type name or dict of column -> type, optional
    #
    # Data type for data or columns. E.g. {‘a’: np.float64, ‘b’:
    # np.int32, ‘c’: ‘Int64’}
    #
    # Use str or object together with suitable na_values settings to
    # preserve and not interpret dtype. If converters are specified,
    # they will be applied INSTEAD of dtype conversion.
    #

    # (not supported now, but this would be great for using
    # sniffed/pre-supplied records schema JSON)

    #
    # engine : {‘c’, ‘python’}, optional
    #
    # Parser engine to use. The C engine is faster while the python
    # engine is currently more feature-complete.
    #

    non_standard_record_terminator = hints.record_terminator not in ['\n', '\r\n', '\r']

    if non_standard_record_terminator:
        # the 'lineterminator' option below is only valid for c parser
        pandas_options['engine'] = 'c'
    else:
        pandas_options['engine'] = 'python'

    # (stick with the default until we have reason to change or
    # dynamically adapt)

    #
    # converters : dict, optional
    #
    # Dict of functions for converting values in certain columns. Keys
    # can either be integers or column labels.
    #

    # (maybe a way we can support a richer set of records hints in the
    # future?)

    #
    # true_values : list, optional
    #
    # Values to consider as True.
    #

    # (not currently specified by records hints - maybe it should be)

    #
    # false_values : list, optional
    #
    # Values to consider as False.
    #

    # (not currently specified by records hints - maybe it should be)

    #
    # skipinitialspace : bool, default False
    #
    # Skip spaces after delimiter.
    #

    # (not currently specified by records hints - maybe it should be)

    #
    # skiprows : list-like, int or callable, optional
    #
    # Line numbers to skip (0-indexed) or number of lines to skip
    # (int) at the start of the file.
    #
    # If callable, the callable function will be evaluated against the
    # row indices, returning True if the row should be skipped and
    # False otherwise. An example of a valid callable argument would
    # be lambda x: x in [0, 2].
    #

    # (this may be useful when chunking through a file to avoid
    # blowing up memory)

    #
    # skipfooter : int, default 0
    #
    # Number of lines at bottom of file to skip (Unsupported with
    # engine=’c’).
    #

    # (this may be useful when chunking through a file to avoid
    # blowing up memory)

    #
    # nrows : int, optional
    #
    # Number of rows of file to read. Useful for reading pieces of large files.
    #

    # (this may be useful when chunking through a file to avoid
    # blowing up memory)

    #
    # na_values : scalar, str, list-like, or dict, optional
    #
    # Additional strings to recognize as NA/NaN. If dict passed,
    # specific per-column NA values. By default the following values
    # are interpreted as NaN: ‘’, ‘#N/A’, ‘#N/A N/A’, ‘#NA’,
    # ‘-1.#IND’, ‘-1.#QNAN’, ‘-NaN’, ‘-nan’, ‘1.#IND’, ‘1.#QNAN’,
    # ‘N/A’, ‘NA’, ‘NULL’, ‘NaN’, ‘n/a’, ‘nan’, ‘null’.
    #

    # (not fully specified in spec yet, though some work was started
    # at one point)

    #
    # keep_default_na : bool, default True
    #
    # Whether or not to include the default NaN values when parsing
    # the data. Depending on whether na_values is passed in, the
    # behavior is as follows:
    #
    # If keep_default_na is True, and na_values are specified,
    # na_values is appended to the default NaN values used for
    # parsing.
    #
    # If keep_default_na is True, and na_values are not specified,
    # only the default NaN values are used for parsing.
    #
    # If keep_default_na is False, and na_values are specified, only
    # the NaN values specified na_values are used for parsing.
    #
    # If keep_default_na is False, and na_values are not specified, no
    # strings will be parsed as NaN.
    #
    # Note that if na_filter is passed in as False, the
    # keep_default_na and na_values parameters will be ignored.
    #

    # (not fully specified in spec yet, though some work was started
    # at one point)

    #
    # na_filter : bool, default True
    #
    # Detect missing value markers (empty strings and the value of
    # na_values). In data without any NAs, passing na_filter=False can
    # improve the performance of reading a large file.
    #

    # (not fully specified in spec yet, though some work was started
    # at one point)

    #
    # verbose : bool, default False
    #
    # Indicate number of NA values placed in non-numeric columns.
    #

    # (might be useful if we provided some easy verbosity knobs when
    # using records mover)

    #
    # skip_blank_lines : bool, default True
    #
    # If True, skip over blank lines rather than interpreting as NaN
    # values.
    #

    # (maybe someday we'll have a relevant hint, but for now, use
    # whatever pandas' default behavior is)

    #
    # parse_dates : bool or list of int or names or list of lists or
    # dict, default False
    #
    # The behavior is as follows:
    #
    # boolean. If True -> try parsing the index.
    #
    # list of int or names. e.g. If [1, 2, 3] -> try parsing columns
    # 1, 2, 3 each as a separate date column.
    #
    # list of lists. e.g. If [[1, 3]] -> combine columns 1 and 3 and
    # parse as a single date column.
    #
    # dict, e.g. {‘foo’ : [1, 3]} -> parse columns 1, 3 as date and
    # call result ‘foo’
    #
    # If a column or index cannot be represented as an array of
    # datetimes, say because of an unparseable value or a mixture of
    # timezones, the column or index will be returned unaltered as an
    # object data type. For non-standard datetime parsing, use
    # pd.to_datetime after pd.read_csv. To parse an index or column
    # with a mixture of timezones, specify date_parser to be a
    # partially-applied pandas.to_datetime() with utc=True. See
    # Parsing a CSV with mixed Timezones for more.
    #
    # Note: A fast-path exists for iso8601-formatted dates.
    #

    pandas_options['parse_dates'] = [
        index
        for index, field
        in enumerate(records_schema.fields)
        if field.field_type in ['date', 'time', 'datetime', 'datetimetz']
    ]

    #
    # infer_datetime_format : bool, default False
    #
    # If True and parse_dates is enabled, pandas will attempt to infer
    # the format of the datetime strings in the columns, and if it can
    # be inferred, switch to a faster method of parsing them. In some
    # cases this can increase the parsing speed by 5-10x.
    #

    # Left as default for now because presumably Pandas has some
    # reason why this isn't the default that they didn't spell out in
    # the docs.

    #
    # keep_date_col : bool, default False
    #
    # If True and parse_dates specifies combining multiple columns
    # then keep the original columns.
    #

    # (N/A as we don't specify combining multiple columns in
    # parse_dates)

    #
    # date_parser : function, optional
    #
    # Function to use for converting a sequence of string columns to
    # an array of datetime instances. The default uses
    # dateutil.parser.parser to do the conversion. Pandas will try to
    # call date_parser in three different ways, advancing to the next
    # if an exception occurs: 1) Pass one or more arrays (as defined
    # by parse_dates) as arguments; 2) concatenate (row-wise) the
    # string values from the columns defined by parse_dates into a
    # single array and pass that; and 3) call date_parser once for
    # each row using one or more strings (corresponding to the columns
    # defined by parse_dates) as arguments.
    #

    # (So far the default parser has handled what we've thrown at it,
    # so we'll leave this at the default)

    #
    # dayfirst : bool, default False
    #
    # DD/MM format dates, international and European format.
    #

    def day_first(dateish_format: str) -> bool:
        return (dateish_format.startswith('DD-MM-') or
                dateish_format.startswith('DD/MM/'))

    assert isinstance(hints.dateformat, str)
    assert isinstance(hints.datetimeformat, str)
    assert isinstance(hints.datetimeformattz, str)
    consistent_formats = (day_first(hints.dateformat) ==
                          day_first(hints.datetimeformat) ==
                          day_first(hints.datetimeformattz))

    if not consistent_formats:
        cant_handle_hint(fail_if_cant_handle_hint, 'dateformat', hints)

    pandas_options['dayfirst'] = day_first(hints.dateformat)

    quiet_remove(unhandled_hints, 'dateformat')
    quiet_remove(unhandled_hints, 'timeonlyformat')
    quiet_remove(unhandled_hints, 'datetimeformat')
    quiet_remove(unhandled_hints, 'datetimeformattz')

    #
    # iterator : bool, default False
    #
    # Return TextFileReader object for iteration or getting chunks
    # with get_chunk()
    #

    # (may be very useful for reading large files - see
    # https://stackoverflow.com/questions/39386458/how-to-read-data-in-python-dataframe-without-concatenating)

    #
    # chunksize : int, optional
    #
    # Return TextFileReader object for iteration. See the IO Tools
    # docs for more information on iterator and chunksize.
    #

    # (may be very useful for reading large files - see
    # https://stackoverflow.com/questions/39386458/how-to-read-data-in-python-dataframe-without-concatenating)

    #
    # compression : {‘infer’, ‘gzip’, ‘bz2’, ‘zip’, ‘xz’, None},
    # default ‘infer’
    #
    # For on-the-fly decompression of on-disk data. If ‘infer’ and
    # filepath_or_buffer is path-like, then detect compression from
    # the following extensions: ‘.gz’, ‘.bz2’, ‘.zip’, or ‘.xz’
    # (otherwise no decompression). If using ‘zip’, the ZIP file must
    # contain only one data file to be read in. Set to None for no
    # decompression.
    #
    # New in version 0.18.1: support for ‘zip’ and ‘xz’ compression.
    #
    if hints.compression is None:
        # hints.compression=None will output an uncompressed csv,
        # which is the pandas default.
        pandas_options['compression'] = None
    elif hints.compression == 'GZIP':
        pandas_options['compression'] = 'gzip'
    elif hints.compression == 'BZIP':
        pandas_options['compression'] = 'bz2'
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'compression', hints)
    quiet_remove(unhandled_hints, 'compression')

    #
    # thousands : str, optional
    #
    # Thousands separator.
    #
    # decimal : str, default ‘.’
    #

    # (not currently specified by records hints - maybe it should be)

    #
    # Character to recognize as decimal point (e.g. use ‘,’ for
    # European data).
    #

    # (not currently specified by records hints - maybe it should be)

    #
    # lineterminator : str (length 1), optional
    #
    # Character to break file into lines. Only valid with C parser.
    #
    if non_standard_record_terminator:
        pandas_options['lineterminator'] = hints.record_terminator
    quiet_remove(unhandled_hints, 'record-terminator')

    #
    # quotechar : str (length 1), optional
    #
    # The character used to denote the start and end of a quoted
    # item. Quoted items can include the delimiter and it will be
    # ignored.
    #
    pandas_options['quotechar'] = hints.quotechar
    quiet_remove(unhandled_hints, 'quotechar')

    #
    # quoting : int or csv.QUOTE_* instance, default 0
    #
    # Control field quoting behavior per csv.QUOTE_* constants. Use
    # one of QUOTE_MINIMAL (0), QUOTE_ALL (1), QUOTE_NONNUMERIC (2) or
    # QUOTE_NONE (3).
    #

    if hints.quoting is None:
        pandas_options['quoting'] = csv.QUOTE_NONE
    elif hints.quoting == 'all':
        pandas_options['quoting'] = csv.QUOTE_ALL
    elif hints.quoting == 'minimal':
        pandas_options['quoting'] = csv.QUOTE_MINIMAL
    elif hints.quoting == 'nonnumeric':
        pandas_options['quoting'] = csv.QUOTE_NONNUMERIC
    else:
        cant_handle_hint(fail_if_cant_handle_hint, 'quoting', hints)
    quiet_remove(unhandled_hints, 'quoting')

    #
    # doublequote : bool, default True
    #
    # When quotechar is specified and quoting is not QUOTE_NONE,
    # indicate whether or not to interpret two consecutive quotechar
    # elements INSIDE a field as a single quotechar element.
    #
    pandas_options['doublequote'] = hints.doublequote
    quiet_remove(unhandled_hints, 'doublequote')

    #
    # escapechar : str (length 1), optional
    #
    # One-character string used to escape other characters.
    #
    if hints.escape is None:
        pass
    else:
        pandas_options['escapechar'] = hints.escape
    quiet_remove(unhandled_hints, 'escape')

    #
    # comment : str, optional
    #
    # Indicates remainder of line should not be parsed. If found at
    # the beginning of a line, the line will be ignored
    # altogether. This parameter must be a single character. Like
    # empty lines (as long as skip_blank_lines=True), fully commented
    # lines are ignored by the parameter header but not by
    # skiprows. For example, if comment='#', parsing
    # #empty\na,b,c\n1,2,3 with header=0 will result in ‘a,b,c’ being
    # treated as the header.
    #

    # (not currently specified by records hints - maybe it should be)

    #
    # encoding : str, optional
    #
    # Encoding to use for UTF when reading/writing (ex. ‘utf-8’). List
    # of Python standard encodings .
    #

    # should only be specified when reading from a filepath
    #
    if hints.compression is not None:
        pandas_options['encoding'] = hints.encoding
    quiet_remove(unhandled_hints, 'encoding')

    #
    # dialect : str or csv.Dialect, optional
    #
    # If provided, this parameter will override values (default or
    # not) for the following parameters: delimiter, doublequote,
    # escapechar, skipinitialspace, quotechar, and quoting. If it is
    # necessary to override values, a ParserWarning will be
    # issued. See csv.Dialect documentation for more details.
    #

    # (we specify the other arguments directly except for
    # skipinitialspace, which isn't currently supported by hints)

    #
    # tupleize_cols : bool, default False
    #
    # Leave a list of tuples on columns as is (default is to convert
    # to a MultiIndex on the columns).
    #
    # Deprecated since version 0.21.0: This argument will be removed
    # and will always convert to MultiIndex
    #

    # (deprecated, so not supplying)

    #
    # error_bad_lines : bool, default True
    #
    # Lines with too many fields (e.g. a csv line with too many
    # commas) will by default cause an exception to be raised, and no
    # DataFrame will be returned. If False, then these “bad lines”
    # will dropped from the DataFrame that is returned.
    #

    pandas_options['error_bad_lines'] = processing_instructions.fail_if_row_invalid

    #
    # warn_bad_lines : bool, default True
    #
    # If error_bad_lines is False, and warn_bad_lines is True, a
    # warning for each “bad line” will be output.
    #

    pandas_options['warn_bad_lines'] = True

    #
    # delim_whitespace : bool, default False
    #
    # Specifies whether or not whitespace (e.g. ' ' or ' ') will be
    # used as the sep. Equivalent to setting sep='\s+'. If this option
    # is set to True, nothing should be passed in for the delimiter
    # parameter.
    #
    # New in version 0.18.1: support for the Python parser.
    #

    # (not currently expressible in hints)

    #
    # low_memory : bool, default True
    #
    # Internally process the file in chunks, resulting in lower memory
    # use while parsing, but possibly mixed type inference. To ensure
    # no mixed types either set False, or specify the type with the
    # dtype parameter. Note that the entire file is read into a single
    # DataFrame regardless, use the chunksize or iterator parameter to
    # return the data in chunks. (Only valid with C parser).
    #

    # (We'll see if we see issues without changing the default on this
    # one)

    #
    # memory_map : bool, default False
    #
    # If a filepath is provided for filepath_or_buffer, map the file
    # object directly onto memory and access the data directly from
    # there. Using this option can improve performance because there
    # is no longer any I/O overhead.
    #

    # (I suspect there's a reason this isn't the default, but it may
    # be a knob to play with if we need more performance...)

    #
    # float_precision : str, optional
    #
    # Specifies which converter the C engine should use for
    # floating-point values. The options are None for the ordinary
    # converter, high for the high-precision converter, and round_trip
    # for the round-trip converter.
    #

    # (something to look for if we need more performance...)

    return pandas_options
