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
