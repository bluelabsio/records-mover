from contextlib import contextmanager
from typing import IO, Iterator
import logging


logger = logging.getLogger(__name__)


@contextmanager
def rewound_fileobj(fileobj: IO[bytes]) -> Iterator[IO[bytes]]:
    if getattr(fileobj, 'closed', None) is not None:
        closed = fileobj.closed
    if closed:
        logger.warning("Stream already closed")
        raise OSError('Stream is already closed')
    if not fileobj.seekable():
        # OSError is what is thrown when you call .seek() on a
        # non-rewindable stream.
        raise OSError('Stream is not rewindable')
    original_position = fileobj.tell()
    fileobj.seek(0)
    try:
        yield fileobj
    finally:
        fileobj.seek(original_position)
