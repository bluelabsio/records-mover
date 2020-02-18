from .base import SupportsRecordsDirectory
from typing import Optional
from ..records_directory import RecordsDirectory
from ...url.resolver import UrlResolver
from ..records_format import BaseRecordsFormat
from ..errors import RecordsFolderNonEmptyException


class DirectoryFromUrlRecordsTarget(SupportsRecordsDirectory):
    def __init__(self,
                 output_url: str,
                 url_resolver: UrlResolver,
                 records_format: Optional[BaseRecordsFormat]) -> None:
        if not output_url.endswith('/'):
            raise ValueError("Please provide a directory name - "
                             f"URL should end with '/': {output_url}")
        self.output_loc = url_resolver.directory_url(output_url)
        self.records_format = records_format

    def validate(self) -> None:
        if (len(self.output_loc.files_in_directory()) > 0):
            message = f"Target directory {self.output_loc.url} is non-empty."
            raise RecordsFolderNonEmptyException(message)

    def records_directory(self) -> RecordsDirectory:
        return RecordsDirectory(records_loc=self.output_loc)
