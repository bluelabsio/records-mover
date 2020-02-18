from contextlib import contextmanager
import os
from urllib.parse import urlparse, unquote
import tempfile
from pathlib import Path
from .base import BaseDirectoryUrl, BaseFileUrl
from typing import IO, Iterator, List, Union, Optional


class BaseFilesystemUrl:
    def __init__(self, url: str, **kwargs) -> None:
        self._parsed_url = urlparse(url)
        self.scheme = self._parsed_url.scheme
        if self._parsed_url.netloc != '':
            raise ValueError(f"Could not understand URL {url}")
        # https://docs.python.org/2/library/urlparse.html
        #
        # "The components are not broken up in smaller parts (for
        # example, the network location is a single string), and %
        # escapes are not expanded."
        self.local_file_path = unquote(self._parsed_url.path)
        self.url = url


class FilesystemFileUrl(BaseFilesystemUrl, BaseFileUrl):
    def __init__(self, url: str, **kwargs) -> None:
        super().__init__(url=url, **kwargs)
        self.basename = os.path.basename(self.local_file_path)

    def open(self, mode: str = "rb") -> IO[bytes]:
        return open(self.local_file_path, mode)

    def rename_to(self, new: 'BaseFileUrl') -> 'FilesystemFileUrl':
        if not isinstance(new, FilesystemFileUrl):
            raise TypeError(f'Can only rename to same type, not {new}')
        os.rename(self.local_file_path, new.local_file_path)
        return new

    def filename(self) -> str:
        "Filename, stripped of any directory information"
        return self.basename

    def delete(self) -> None:
        os.remove(self.local_file_path)

    def size(self) -> int:
        statinfo = os.stat(self.local_file_path)
        return statinfo.st_size

    def containing_directory(self) -> 'FilesystemDirectoryUrl':
        parent_dir = os.path.dirname(self.local_file_path)
        new_url = Path(parent_dir).as_uri()
        return FilesystemDirectoryUrl(new_url)


class FilesystemDirectoryUrl(BaseFilesystemUrl, BaseDirectoryUrl):
    def is_directory(self) -> bool:
        return True

    def file_in_this_directory(self, filename: str) -> 'FilesystemFileUrl':
        new_local_file_path = os.path.join(self.local_file_path, filename)
        new_url = Path(new_local_file_path).as_uri()
        return FilesystemFileUrl(new_url)

    def directory_in_this_directory(self, filename: str) -> 'FilesystemDirectoryUrl':
        new_local_file_path = os.path.join(self.local_file_path, filename)
        new_url = Path(new_local_file_path).as_uri()
        return FilesystemDirectoryUrl(new_url)

    def files_in_directory(self) -> List['BaseFileUrl']:
        return [
            self.file_in_this_directory(f)
            for f in os.listdir(self.local_file_path)
            if os.path.isfile(f"{self.local_file_path}/{f}")
        ]

    def files_and_directories_in_directory(self) -> List[Union[BaseFileUrl, BaseDirectoryUrl]]:
        def entry(filename: str) -> Optional[Union[BaseFileUrl, BaseDirectoryUrl]]:
            path = os.path.join(self.local_file_path, filename)
            if os.path.isfile(path):
                return self.file_in_this_directory(filename)
            elif os.path.isdir(path):
                return self.directory_in_this_directory(filename)
            else:
                return None

        return [
            f
            for f in [entry(filename) for filename in os.listdir(self.local_file_path)]
            if f is not None
        ]

    @contextmanager
    def temporary_directory(self: 'FilesystemDirectoryUrl') -> Iterator['FilesystemDirectoryUrl']:
        with tempfile.TemporaryDirectory(dir=self.local_file_path,
                                         prefix="filesystem_py_") as tempdir:
            new_url = Path(tempdir).as_uri()
            yield FilesystemDirectoryUrl(new_url)
