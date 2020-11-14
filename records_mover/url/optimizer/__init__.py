from contextlib import contextmanager
from records_mover.url.base import BaseDirectoryUrl
import logging
from .gcp_data_transfer_service import GcpDataTransferService
from .s3_copy_via_aws_cli import S3CopyViaAwsCli
from typing import Iterator, Tuple


logger = logging.getLogger(__name__)


class CopyOptimizer:
    """When Records Mover needs to move data between different URL
    schemes, the default way (a single-threaded downloading of bytes
    from the source and then reuploading of bytes to the target) can
    be very slow for large directories.

    This class can use faster means (ideally a cloud-based service
    that can copy things at scale, or at least vendor-optimized
    multi-threaded upload/download code) when they are available.

    This depends on the specifics of the copy, and as such return
    values must be checked to determine if the copy was able to be
    performed.
    """

    def __init__(self) -> None:
        self._gcp_data_transfer = GcpDataTransferService()
        self._s3_copy_via_aws_cli = S3CopyViaAwsCli()

    def copy(self,
             loc: BaseDirectoryUrl,
             other_loc: BaseDirectoryUrl) -> bool:
        """Copy from one directory to another using optimized means.

        :return: True if the copy was performed, or False if no
        optimized means were possible."""
        if loc.scheme == 's3' and other_loc.scheme == 'file':
            from records_mover.url.filesystem import FilesystemDirectoryUrl
            from records_mover.url.s3.s3_directory_url import S3DirectoryUrl

            assert isinstance(loc, S3DirectoryUrl)
            assert isinstance(other_loc, FilesystemDirectoryUrl)
            return self._s3_copy_via_aws_cli.copy(loc, other_loc)
        elif loc.scheme == 's3' and other_loc.scheme == 'gs':
            from records_mover.url.gcs.gcs_directory_url import GCSDirectoryUrl
            from records_mover.url.s3.s3_directory_url import S3DirectoryUrl

            assert isinstance(loc, S3DirectoryUrl)
            assert isinstance(other_loc, GCSDirectoryUrl)
            return self._gcp_data_transfer.copy(loc, other_loc)
        else:
            logger.info(f"No strategy to optimize copy from {loc} to {other_loc}")
            return False

    @contextmanager
    def optimize_temp_second_location(self,
                                      permanent_first_loc: BaseDirectoryUrl,
                                      temp_second_loc: BaseDirectoryUrl) ->\
            Iterator[BaseDirectoryUrl]:
        if permanent_first_loc.scheme == 's3' and temp_second_loc.scheme == 'gs':
            from records_mover.url.gcs.gcs_directory_url import GCSDirectoryUrl
            from records_mover.url.s3.s3_directory_url import S3DirectoryUrl

            assert isinstance(permanent_first_loc, S3DirectoryUrl)
            assert isinstance(temp_second_loc, GCSDirectoryUrl)

            with self._gcp_data_transfer.\
                optimize_temp_second_location(permanent_first_loc,
                                              temp_second_loc) as\
                    optimized_second_loc:
                logger.info(f"Optimized bucket location: {optimized_second_loc}")
                yield optimized_second_loc
        elif permanent_first_loc.scheme == temp_second_loc.scheme:
            # Let's use the same location!
            yield permanent_first_loc
        else:
            # No optimizations match
            yield temp_second_loc

    @contextmanager
    def optimize_temp_locations(self,
                                temp_first_loc: BaseDirectoryUrl,
                                temp_second_loc: BaseDirectoryUrl) ->\
            Iterator[Tuple[BaseDirectoryUrl, BaseDirectoryUrl]]:
        # TODO document this and other methods
        if temp_first_loc.scheme == 's3' and temp_second_loc.scheme == 'gs':
            from records_mover.url.gcs.gcs_directory_url import GCSDirectoryUrl
            from records_mover.url.s3.s3_directory_url import S3DirectoryUrl

            assert isinstance(temp_first_loc, S3DirectoryUrl)
            assert isinstance(temp_second_loc, GCSDirectoryUrl)

            with self._gcp_data_transfer.\
                optimize_temp_locations(temp_first_loc,
                                        temp_second_loc) as\
                    (optimized_first_loc, optimized_second_loc):
                logger.info(f"Optimized bucket locations: {optimized_first_loc}, "
                            f"{optimized_second_loc}")
                yield (optimized_first_loc, optimized_second_loc)
        elif temp_first_loc.scheme == temp_second_loc.scheme:
            # Let's use the same location!
            yield (temp_first_loc, temp_first_loc)
        else:
            # No optimizations match
            yield (temp_first_loc, temp_second_loc)
