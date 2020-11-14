from contextlib import contextmanager
import secrets
import logging
import json
from records_mover.mover_types import JsonValue
from typing import TypeVar, Iterator, IO, Any, Optional, List, Union

V = TypeVar('V', bound='BaseDirectoryUrl')


# Adapted from
# https://github.com/python/cpython/blob/3.7/Lib/shutil.py
# but this one returns the size copied
def blcopyfileobj(fsrc: IO[bytes], fdst: IO[bytes], length: int = 16*1024) -> int:
    """copy data from file-like object fsrc to file-like object fdst"""
    written = 0
    while 1:
        buf = fsrc.read(length)
        if not buf:
            break
        written = written + fdst.write(buf)
    return written


class BaseDirectoryUrl:
    scheme: str
    url: str

    def __init__(self, url: str, **kwargs) -> None:
        raise NotImplementedError()

    def _file(self, url: str) -> 'BaseFileUrl':
        raise NotImplementedError

    def directory_in_this_directory(self: V, directory_name: str) -> V:
        "Create a subdirectory within this file's current directory with the given name"
        raise NotImplementedError()

    def file_in_this_directory(self, filename: str) -> 'BaseFileUrl':
        "Return an entry in the directory."
        return self._file(self.url + filename)

    def files_in_directory(self) -> List['BaseFileUrl']:
        "Return entries in this directory."
        raise NotImplementedError()

    def directories_in_directory(self) -> List['BaseDirectoryUrl']:
        "Return entries in this directory."
        raise NotImplementedError()

    def files_and_directories_in_directory(self) -> List[Union['BaseFileUrl', 'BaseDirectoryUrl']]:
        "Return all file and folder entries directly under the current location.  Does not recurse."
        out: List[Union['BaseFileUrl', 'BaseDirectoryUrl']] = []
        out.extend(self.files_in_directory())
        out.extend(self.directories_in_directory())
        return out

    def purge_directory(self) -> None:
        "Delete all entries and subdirectories in the directory."
        raise NotImplementedError()

    def copy_to(self, other_loc: 'BaseDirectoryUrl') -> 'BaseDirectoryUrl':
        "Copy all entries to the specified directory and return it"
        for file_or_directory in self.files_and_directories_in_directory():
            if file_or_directory.is_directory():
                source_subdirectory: BaseDirectoryUrl = file_or_directory  # type: ignore
                other_subdirname = source_subdirectory.filename()
                other_subdirectory = other_loc.directory_in_this_directory(other_subdirname)
                source_subdirectory.copy_to(other_subdirectory)
            else:
                source_file: BaseFileUrl = file_or_directory  # type: ignore
                other_filename = source_file.filename()
                other_file = other_loc.file_in_this_directory(other_filename)
                source_file.copy_to(other_file)
        return other_loc

    def is_directory(self) -> bool:
        "Returns true"
        return self.url.endswith('/')

    def containing_directory(self: V) -> V:
        "Parent directory of this directory"
        raise NotImplementedError()

    @contextmanager
    def temporary_directory(self: V) -> Iterator[V]:
        "Yields a temporary DirectoryUrl in current location"
        num_chars = 8
        random_slug = secrets.token_urlsafe(num_chars)
        temp_loc = self.directory_in_this_directory(random_slug)
        try:
            yield temp_loc
        finally:
            temp_loc.purge_directory()

    def filename(self) -> str:
        "Filename, stripped of any directory information"
        raise NotImplementedError()

    def files_matching_prefix(self, prefix: str) -> List['BaseFileUrl']:
        "entries in the directory that match the prefix in its name."
        return [f for f in self.files_in_directory() if f.filename().startswith(prefix)]

    def __str__(self) -> str:
        return self.url

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.url})"


T = TypeVar('T', bound='BaseFileUrl')


class BaseFileUrl:
    scheme: str
    url: str

    def __init__(self, url: str, **kwargs) -> None:
        raise NotImplementedError()

    def _directory(self, url: str) -> BaseDirectoryUrl:
        raise NotImplementedError()

    def directory_in_this_directory(self, directory_name: str) -> BaseDirectoryUrl:
        "Create a subdirectory within this file's current directory with the given name"
        raise NotImplementedError()

    def string_contents(self, encoding: str = 'utf-8') -> str:
        "Return the contents of this file as a string."
        with self.open() as f:
            return f.read().decode(encoding)

    def json_contents(self) -> Optional[JsonValue]:
        """Return contents of this JSON file as a Python object, or None if file is empty
        (zero bytes)"""
        file_contents = self.string_contents()
        if len(file_contents) > 0:
            data = json.loads(file_contents)
        else:
            data = None
        return data

    def file_in_this_directory(self, filename: str) -> 'BaseFileUrl':
        "Find a given file within this directory."
        raise NotImplementedError()

    def upload_fileobj(self, fileobj: IO[bytes], mode: str = 'wb') -> int:
        "Put the contents of the fileobj (stream) into the current file and "
        "return the number of bytes written."
        with self.open(mode=mode) as local_file:
            return blcopyfileobj(fileobj, local_file)

    def download_fileobj(self, output_fileobj: IO[bytes]) -> None:
        with self.open() as f:
            blcopyfileobj(f, output_fileobj)

    def store_string(self, contents: str) -> None:
        "Save the specified string to the file."

        with self.open(mode='wb') as f:
            f.write(contents.encode('utf-8'))

    def rename_to(self, new: 'BaseFileUrl') -> 'BaseFileUrl':
        "Rename this file"
        raise NotImplementedError()

    def is_directory(self) -> bool:
        "Returns false"
        return False

    def concatenate_from(self, other_locs: List['BaseFileUrl']) -> Optional[int]:
        length = 0
        with self.open(mode='wb') as output_fileobj:
            for input_loc in other_locs:
                with input_loc.open(mode='rb') as input_fileobj:
                    length += blcopyfileobj(input_fileobj, output_fileobj)
        return length

    def copy_to(self, other_loc: 'BaseFileUrl') -> 'BaseFileUrl':
        "Copy to the specified file and return it"
        with self.open() as fileobj:
            other_loc.upload_fileobj(fileobj)
        return other_loc

    def containing_directory(self) -> BaseDirectoryUrl:
        "Returns the parent directory of this file."
        parent_url = '/'.join(self.url.split('/')[:-1]) + '/'
        return self._directory(parent_url)

    @contextmanager
    def temporary_directory(self) -> Iterator[BaseDirectoryUrl]:
        parent_dir_loc = self.containing_directory()
        with parent_dir_loc.temporary_directory() as dir:
            yield dir

    def filename(self) -> str:
        "Filename, stripped of any directory information"
        raise NotImplementedError()

    def open(self, mode: str = "rb") -> IO[Any]:
        "Open file with the given mode.  Raises FileNotFoundError if file does not "
        "exist in the directory."
        raise NotImplementedError()

    def wait_to_exist(self,
                      log_level: int = logging.INFO,
                      ms_between_polls: int = 50) -> None:
        "Returns after the file exists--useful for eventually consistent stores (e.g., S3)"
        return

    def exists(self) -> bool:
        try:
            with self.open():
                return True
        except FileNotFoundError:
            return False

    def delete(self) -> None:
        raise NotImplementedError(f"Please implement for {type(self).__name__}")

    def size(self) -> int:
        raise NotImplementedError(f"Please implement for {type(self).__name__}")

    def __str__(self) -> str:
        return self.url

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.url})"
