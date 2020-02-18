from urllib.parse import urlparse
import os
from typing import Optional


def sniff_compression_from_url(url: str) -> Optional[str]:
    "Returns compression hint for a given file extension"
    urlobj = urlparse(url)
    pathname = urlobj.path
    if pathname:
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
