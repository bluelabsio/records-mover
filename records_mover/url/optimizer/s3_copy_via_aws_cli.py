
from contextlib import contextmanager
from records_mover.url.base import BaseDirectoryUrl
import logging
from .gcp_data_transfer_service import GcpDataTransferService
from typing import Iterator, Tuple, TYPE_CHECKING
if TYPE_CHECKING:
    from records_mover.url.s3.s3_directory_url import S3DirectoryUrl
    from records_mover.url.filesystem import FilesystemDirectoryUrl

logger = logging.getLogger(__name__)


class S3CopyViaAwsCli:
    def copy(self,
             loc: 'S3DirectoryUrl',
             other_loc: 'FilesystemDirectoryUrl') -> bool:
        from .awscli import aws_cli

        # TODO: Can I pass in creds?
        aws_cli('s3', 'sync', loc.url, other_loc.local_file_path)
        return True
