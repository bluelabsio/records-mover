import json
import time
import datetime
from contextlib import contextmanager
from records_mover.url.base import BaseDirectoryUrl
import logging
from typing import Iterator, Tuple, Union, Optional, TYPE_CHECKING
if TYPE_CHECKING:
    from googleapiclient.discovery import _TransferJobConfig
    import google.auth.credentials
    from records_mover.url.gcs.gcs_directory_url import GCSDirectoryUrl
    from records_mover.url.s3.s3_directory_url import S3DirectoryUrl
    from records_mover.url.filesystem import FilesystemDirectoryUrl


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
            return self._copy_via_awscli(loc, other_loc)
        elif loc.scheme == 's3' and other_loc.scheme == 'gs':
            from records_mover.url.gcs.gcs_directory_url import GCSDirectoryUrl
            from records_mover.url.s3.s3_directory_url import S3DirectoryUrl

            assert isinstance(loc, S3DirectoryUrl)
            assert isinstance(other_loc, GCSDirectoryUrl)
            return self._copy_via_gcp_data_transfer(loc, other_loc)
        else:
            logger.info(f"No strategy to optimize copy from {loc} to {other_loc}")
            return False

    def _copy_via_awscli(self,
                         loc: 'S3DirectoryUrl',
                         other_loc: 'FilesystemDirectoryUrl') -> bool:
        from .awscli import aws_cli

        # TODO: Can I pass in creds?
        aws_cli('s3', 'sync', loc.url, other_loc.local_file_path)
        return True

    def _copy_via_gcp_data_transfer(self,
                                    loc: 'S3DirectoryUrl',
                                    other_loc: 'GCSDirectoryUrl') -> bool:
        """Use the Google Cloud Platform Data Transfer Service to copy from S3
        to GCS.

        To perform this, we need the key on the S3 side to match the
        blob on the GCS side, as Google Cloud Platform Data Transfer
        Service does not allow the destination on the GCS side to vary
        as of 2020-11.

        We also need to be using non-temporary AWS credentials; Google
        Cloud Platform Data Transfer Service doesn't accept an AWS
        security token.

        :return: True if the copy was performed, or False if no
        optimized means were possible.

        """
        import googleapiclient

        # https://cloud.google.com/storage-transfer/docs/create-manage-transfer-program#python
        if loc.key != other_loc.blob:
            logger.warning("S3 directory does not match GCS directory - "
                           "cannot copy S3 bucket using Google Cloud Platform Data Transfer Service.  "
                           "Falling back to a slower method of bucket copy.")
            return False

        gcp_credentials = other_loc.credentials
        storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1',
                                                          credentials=gcp_credentials)
        description = "records-mover one-time job"
        project_id = other_loc.gcp_project_id
        now = datetime.datetime.utcnow()
        current_day = now.day
        current_month = now.month
        current_year = now.year
        sink_bucket = other_loc.bucket
        source_bucket = loc.bucket
        aws_creds = loc.aws_creds()
        if aws_creds is None:
            logger.warning("S3 bucket did not provide AWS creds - "
                           "cannot copy S3 bucket using Google Cloud Platform Data Transfer Service.  "
                           "Falling back to a slower method of bucket copy.")
            return False
        if aws_creds.token is not None:
            logger.warning("S3 bucket is using a temporary access token (MFA creds?) which "
                           "Google Cloud Platform Data Transfer Service does not support.  Falling back to a slower "
                           "method of bucket copy.")
            return False

        access_key_id = aws_creds.access_key
        secret_access_key = aws_creds.secret_key

        # Edit this template with desired parameters.
        transfer_job: '_TransferJobConfig' = {
            'description': description,
            'status': 'ENABLED',
            'projectId': project_id,
            'schedule': {
                'scheduleStartDate': {
                    'day': current_day,
                    'month': current_month,
                    'year': current_year,
                },
                'scheduleEndDate': {
                    'day': current_day,
                    'month': current_month,
                    'year': current_year,
                }
            },
            'transferSpec': {
                'awsS3DataSource': {
                    'bucketName': source_bucket,
                    'awsAccessKey': {
                        'accessKeyId': access_key_id,
                        'secretAccessKey': secret_access_key
                    }
                },
                "objectConditions": {
                    "includePrefixes": [
                        loc.key
                    ]
                },
                "transferOptions": {
                    "overwriteObjectsAlreadyExistingInSink": False,
                    "deleteObjectsUniqueInSink": False,
                    "deleteObjectsFromSourceAfterTransfer": False
                },
                'gcsDataSink': {
                    'bucketName': sink_bucket
                }
            }
        }

        result = storagetransfer.transferJobs().create(body=transfer_job).execute()
        logger.debug('Returned transferJob: {}'.format(
            json.dumps(result, indent=4)))
        job_name = result['name']
        self._wait_for_transfer_job(project_id, job_name, gcp_credentials)
        return True

    def _wait_for_transfer_job(self,
                               project_id: str,
                               job_name: str,
                               gcp_credentials: 'google.auth.credentials.Credentials') -> None:
        import googleapiclient

        storagetransfer = googleapiclient.discovery.build('storagetransfer',
                                                          'v1',
                                                          credentials=gcp_credentials)

        # https://github.com/karthikey-surineni/bigquery-copy-dataset/blob/fb6be5aee9898cba79387a2f95fb54b8b1119a92/src/storage/storage_transfer.py
        filterString = (
            '{{"project_id": "{project_id}", '
            '"job_names": ["{job_name}"]}}'
        ).format(project_id=project_id, job_name=job_name)

        done = False
        logger.info("Awaiting completion of Google Cloud Platform Data Transfer Service job...")
        while not done:
            result = storagetransfer.transferOperations().list(
                name="transferOperations",
                filter=filterString).execute()
            logger.debug('Result of transferOperations/list: {}'.format(
                json.dumps(result, indent=4, sort_keys=True)))
            if result != {}:
                done = result['operations'][0]['metadata']['status'] != 'IN_PROGRESS'
            time.sleep(10)
        logger.info("Google Cloud Platform Data Transfer Service job complete.")

    def try_swapping_bucket_path(self,
                                 target_bucket: Union['GCSDirectoryUrl', 'S3DirectoryUrl'],
                                 bucket_to_steal_path_from: Union['GCSDirectoryUrl',
                                                                  'S3DirectoryUrl']) ->\
            Optional[Union['GCSDirectoryUrl', 'S3DirectoryUrl']]:
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
    def _optimize_temp_locations_for_gcp_data_transfer(self,
                                                       temp_unloadable_loc: 'S3DirectoryUrl',
                                                       temp_loadable_loc: 'GCSDirectoryUrl') ->\
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
                       "will not be able to use Google Cloud Platform Data Transfer Service for "
                       "cloud-based copy.")
        yield (temp_unloadable_loc, temp_loadable_loc)

    @contextmanager
    def optimize_temp_locations(self,
                                temp_unloadable_loc: BaseDirectoryUrl,
                                temp_loadable_loc: BaseDirectoryUrl) ->\
            Iterator[Tuple[BaseDirectoryUrl, BaseDirectoryUrl]]:
        if temp_unloadable_loc.scheme == 's3' and temp_loadable_loc.scheme == 'gs':
            from records_mover.url.gcs.gcs_directory_url import GCSDirectoryUrl
            from records_mover.url.s3.s3_directory_url import S3DirectoryUrl

            assert isinstance(temp_unloadable_loc, S3DirectoryUrl)
            assert isinstance(temp_loadable_loc, GCSDirectoryUrl)

        if (isinstance(temp_unloadable_loc, S3DirectoryUrl) and
           isinstance(temp_loadable_loc, GCSDirectoryUrl)):
            with self._optimize_temp_locations_for_gcp_data_transfer(temp_unloadable_loc,
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
