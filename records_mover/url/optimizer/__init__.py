from contextlib import contextmanager
from records_mover.url.base import BaseDirectoryUrl
from records_mover.url.gcs.gcs_directory_url import GCSDirectoryUrl
from records_mover.url.s3.s3_directory_url import S3DirectoryUrl
from typing import Iterator, Tuple, Union, Optional
import logging


logger = logging.getLogger(__name__)


class CopyOptimizer:
    # TODO: move to new location
    # TODO: Bring in Google code
    # TODO: Figure out where this should be called from

    def try_swapping_bucket_path(self,
                                 target_bucket: Union[GCSDirectoryUrl, S3DirectoryUrl],
                                 bucket_to_steal_path_from: Union[GCSDirectoryUrl,
                                                                  S3DirectoryUrl]) ->\
                Optional[Union[GCSDirectoryUrl, S3DirectoryUrl]]:
        optimized_directory =\
            target_bucket.directory_in_this_bucket(bucket_to_steal_path_from.key)
        if not optimized_directory.empty():
            logger.info(f"{optimized_directory} is not empty")
            return None

        if not optimized_directory.writable():
            logger.info(f"{optimized_directory} is not writable")
            return None

        return optimized_directory

    @contextmanager
    def optimize_temp_locations_for_gcp_data_transfer(self,
                                                      temp_unloadable_loc: S3DirectoryUrl,
                                                      temp_loadable_loc: GCSDirectoryUrl) ->\
            Iterator[Tuple[BaseDirectoryUrl, BaseDirectoryUrl]]:
        #
        # GCP data transfer is great, but has the limitations:
        #
        # 1) It only works from GCS -> GCS or S3 -> GCS - like the
        # roach motel, you can check in but you can't check out.
        #
        # 2) You can't specify a different destination location
        # directory in the GCS bucket than in your source bucket
        #
        # So let's make sure that if at all possible, we use the same
        # directory.
        optimized_temp_unloadable_loc = self.try_swapping_bucket_path(temp_unloadable_loc,
                                                                      temp_loadable_loc)
        if optimized_temp_unloadable_loc is not None:
            yield (optimized_temp_unloadable_loc, temp_loadable_loc)
            return
        optimized_temp_loadable_loc = self.try_swapping_bucket_path(temp_loadable_loc,
                                                                    temp_unloadable_loc)
        if optimized_temp_loadable_loc is not None:
            yield (temp_unloadable_loc, optimized_temp_loadable_loc)
            return
        logger.warning("Could not match paths between source and destination buckets--"
                       "will not be able to use Google Storage Transfer Service for "
                       "cloud-based copy.")
        yield (temp_unloadable_loc, temp_loadable_loc)

    @contextmanager
    def optimize_temp_locations(self,
                                temp_unloadable_loc: BaseDirectoryUrl,
                                temp_loadable_loc: BaseDirectoryUrl) ->\
            Iterator[Tuple[BaseDirectoryUrl, BaseDirectoryUrl]]:
        if (isinstance(temp_unloadable_loc, S3DirectoryUrl) and
           isinstance(temp_loadable_loc, GCSDirectoryUrl)):
            with self.optimize_temp_locations_for_gcp_data_transfer(temp_unloadable_loc,
                                                                    temp_loadable_loc) as\
                    (optimized_unloadable_loc, optimized_loadable_loc):
                logger.info(f"Optimized bucket locations: {optimized_unloadable_loc}, "
                            f"{optimized_loadable_loc}")
                yield (optimized_unloadable_loc, optimized_loadable_loc)
        elif temp_unloadable_loc.scheme == temp_loadable_loc.scheme:
            # Let's use the same location!
            yield (temp_unloadable_loc, temp_unloadable_loc)
        else:
            #
            # No optimizations match
            #
            yield (temp_unloadable_loc, temp_loadable_loc)
