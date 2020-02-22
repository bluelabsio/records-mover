from urllib.parse import urlparse
import os
from typing import Optional


def sniff_compression_from_url(url: str) -> Optional[str]:
    urlobj = urlparse(url)
    pathname = urlobj.path
    if pathname:
        return sniff_compression_from_pathname(pathname)
    else:
        return None


def sniff_compression_from_pathname(pathname: str) -> Optional[str]:
    "Returns compression hint for a given file extension"
    splitpathname = os.path.splitext(pathname)
    if len(splitpathname) != 2:
        return None
    ext = splitpathname[1]
    if ext:
        if ext.lower() == '.gz':
            return 'GZIP'
        elif ext.lower() == '.bz2':
            return "BZIP"
        elif ext.lower() == '.lzo':
            return "LZO"
    return None
