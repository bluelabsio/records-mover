SAMPLE_YEAR = 1983
SAMPLE_YEAR_SHORT = 83
SAMPLE_MONTH = 1
SAMPLE_DAY = 2


def create_sample(template: str) -> str:
    return (
        template
        .replace('YYYY', str(SAMPLE_YEAR))
        .replace('YY', ('%02d' % SAMPLE_YEAR_SHORT))
        .replace('MM', ('%02d' % SAMPLE_MONTH))
        .replace('DD', ('%02d' % SAMPLE_DAY))
    )


# TODO: Convert back to date cases probably
DATE_CASES = [
    'YYYY-MM-DD',
    'MM-DD-YYYY',
    'DD-MM-YYYY',
    'MM/DD/YY',
    'DD/MM/YY',
]

TIMEONLY_CASES = [
    "HH12:MI AM",
    "HH24:MI:SS",
]


DATETIMEFORMATTZ_CASES = [
    "YYYY-MM-DD HH:MI:SSOF",
    "YYYY-MM-DD HH:MI:SS",
    "YYYY-MM-DD HH24:MI:SSOF",
    "MM/DD/YY HH24:MI",
]

DATETIMEFORMAT_CASES = [
    "YYYY-MM-DD HH24:MI:SS",
    "YYYY-MM-DD HH:MI:SS",
    "YYYY-MM-DD HH12:MI AM",
    "MM/DD/YY HH24:MI",
]
