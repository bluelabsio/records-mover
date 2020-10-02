bluelabs_format_hints = {
    'field-delimiter': ',',
    'record-terminator': "\n",
    'compression': 'GZIP',
    'quoting': None,
    'quotechar': '"',
    'doublequote': False,
    'escape': '\\',
    'encoding': 'UTF8',
    'dateformat': 'YYYY-MM-DD',
    'timeonlyformat': 'HH24:MI:SS',
    'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
    'datetimeformat': 'YYYY-MM-DD HH24:MI:SS',
    'header-row': False,
}

csv_format_hints = {
    'field-delimiter': ',',
    'record-terminator': "\n",
    'compression': 'GZIP',
    'quoting': 'minimal',
    'quotechar': '"',
    'doublequote': True,
    'escape': None,
    'encoding': 'UTF8',
    'dateformat': 'MM/DD/YY',
    'timeonlyformat': 'HH24:MI:SS',
    'datetimeformattz': 'MM/DD/YY HH24:MI',
    'datetimeformat': 'MM/DD/YY HH24:MI',
    'header-row': True,
}

vertica_format_hints = {
    'field-delimiter': '\001',
    'record-terminator': '\002',
    'compression': None,
    'quoting': None,
    'quotechar': '"',
    'doublequote': False,
    'escape': None,
    'encoding': 'UTF8',
    'dateformat': 'YYYY-MM-DD',
    'timeonlyformat': 'HH24:MI:SS',
    'datetimeformat': 'YYYY-MM-DD HH:MI:SS',
    'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
    'header-row': False,
}

christmas_tree_format_1_hints = {
    'field-delimiter': '\001',
    'record-terminator': '\002',
    'compression': 'LZO',
    'quoting': 'nonnumeric',
    'quotechar': '"',
    'doublequote': False,
    'escape': '\\',
    'encoding': 'UTF8',
    'dateformat': 'YYYY-MM-DD',
    'timeonlyformat': 'HH24:MI:SS',
    'datetimeformat': 'YYYY-MM-DD HH24:MI:SS',
    'datetimeformattz': 'YYYY-MM-DD HH:MI:SSOF',
    'header-row': True,
}

christmas_tree_format_2_hints = {
    'field-delimiter': '\001',
    'record-terminator': '\002',
    'compression': 'BZIP',
    'quoting': 'all',
    'quotechar': '"',
    'doublequote': True,
    'escape': '@',  # not really allowed in the spec, but let's see what happens
    'encoding': 'UTF8',
    'dateformat': 'MM-DD-YYYY',
    'timeonlyformat': 'HH24:MI:SS',
    'datetimeformat': 'YYYY-MM-DD HH24:MI:SS',
    'datetimeformattz': 'HH:MI:SSOF YYYY-MM-DD',  # also not allowed
    'header-row': False,
}

christmas_tree_format_3_hints = {
    'field-delimiter': '\001',
    'record-terminator': '\002',
    'compression': 'BZIP',
    'quoting': 'some_future_option_not_supported_now',
    'quotechar': '"',
    'doublequote': True,
    'escape': '@',  # not really allowed in the spec, but let's see what happens
    'encoding': 'UTF8',
    'dateformat': 'DD-MM-YYYY',
    'timeonlyformat': 'HH24:MI:SS',
    'datetimeformat': 'YYYY-MM-DD HH24:MI:SS',
    'datetimeformattz': 'HH:MI:SSOF YYYY-MM-DD',  # also not allowed
    'header-row': False,
}

christmas_tree_format_4_hints = {
    'field-delimiter': '\001',
    'record-terminator': '\002',
    'compression': 'BZIP',
    'quoting': 'some_future_option_not_supported_now',
    'quotechar': '"',
    'doublequote': True,
    'escape': '@',  # not really allowed in the spec, but let's see what happens
    'encoding': 'UTF8',
    'dateformat': 'totally_bogus_just_made_this_up',
    'timeonlyformat': 'HH24:MI:SS',
    'datetimeformat': 'YYYY-MM-DD HH24:MI:SS',
    'datetimeformattz': 'HH:MI:SSOF YYYY-MM-DD',  # also not allowed
    'header-row': False,
}
