from contextlib import contextmanager
import os
import tempfile
from typing import Iterator


@contextmanager
def set_temp_dir(new_temp_dir: str) -> Iterator[None]:
    old_env = dict(os.environ)
    old_tempdir = tempfile.tempdir
    try:
        # https://docs.python.org/3/library/tempfile.html#tempfile.gettempdir
        env = os.environ.copy()
        env['TMPDIR'] = new_temp_dir
        env['TEMP'] = new_temp_dir
        env['TMP'] = new_temp_dir
        os.environ.update(env)

        tempfile.tempdir = new_temp_dir
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_env)
        tempfile.tempdir = old_tempdir
