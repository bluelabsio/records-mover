import json
import time
import datetime
from config_resolver import get_config
from contextlib import contextmanager
from records_mover.url.base import BaseDirectoryUrl
import logging
from typing import Iterator, Tuple, Union, Optional, TYPE_CHECKING
if TYPE_CHECKING:
    from googleapiclient.discovery import _TransferJobConfig
    import google.auth.credentials
    from records_mover.url.gcs.gcs_directory_url import GCSDirectoryUrl
    from records_mover.url.s3.s3_directory_url import S3DirectoryUrl


logger = logging.getLogger(__name__)


class GcpDataTransferService:
    def _min_bytes_to_use(self) -> int:
        # Number of bytes in an S3 bucket before we break out GCP Data
        # Transfer Service, which does come with some spin-up time
        # that seems independent of bucket size.
        metric_megabyte = 1_000_000
        default_threshold = 500*metric_megabyte
        config_result = get_config('records_mover', 'bluelabs')
        cfg = config_result.config
        gcp_cfg = dict(cfg).get('gcp', {})
        return int(gcp_cfg.get('data_transfer_service_min_bytes_to_use', default_threshold))

    def copy(self,
             loc: 'S3DirectoryUrl',
             other_loc: 'GCSDirectoryUrl') -> bool:
        """Use the Google Cloud Platform Data Transfer Service to copy from S3
        to GCS.

        To perform this, we need the key on the S3 side to match the
        blob on the GCS side, as Google Cloud Platform Data Transfer
        Service does not allow the destination on the GCS side to vary
        as of 2020-11.  Be sure to call optimize_temp_locations() or
        optimize_temp_second_location() beforehand.

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
                           "cannot copy S3 bucket using Google Cloud Platform "
                           "Data Transfer Service.  "
                           "Falling back to a slower method of bucket copy.")
            return False

        directory_size = loc.size()
        min_bytes_to_use = self._min_bytes_to_use()
        if directory_size < min_bytes_to_use:
            # :,d below does formatting of integers with commas every three digits
            logger.info(f"Bucket directory size ({directory_size:,d} bytes) is less "
                        f"than configured minimum size ({min_bytes_to_use:,d} bytes) - skipping "
                        "Google Cloud Platform Data Transfer Service")
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
                           "cannot copy S3 bucket using Google Cloud Platform "
                           "Data Transfer Service.  "
                           "Falling back to a slower method of bucket copy.")
            return False
        if aws_creds.token is not None:
            logger.warning("S3 bucket is using a temporary access token (MFA creds?) which "
                           "Google Cloud Platform "
                           "Data Transfer Service does not support.  Falling back to a slower "
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

        try:
            result = storagetransfer.transferJobs().create(body=transfer_job).execute()
        except googleapiclient.errors.HttpError as e:
            logger.debug("Exception details",
                         exc_info=True)
            logger.warning("Google Cloud Platform Data Transfer Service returned an error.  "
                           f"Falling back to a slower method of bucket copy: {e}")
            return False
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

    def _try_swapping_bucket_path(self,
                                  target_bucket: Union['GCSDirectoryUrl', 'S3DirectoryUrl'],
                                  bucket_to_steal_path_from: Union['GCSDirectoryUrl',
                                                                   'S3DirectoryUrl']) ->\
            Optional[Union['GCSDirectoryUrl', 'S3DirectoryUrl']]:
        # Take the path from bucket_to_steal_path_from, apply it to
        # the bucket in target_bucket, and try a small test write to
        # see if we can use that space as a temporary directory.
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
    def optimize_temp_locations(self,
                                temp_first_loc: 'S3DirectoryUrl',
                                temp_second_loc: 'GCSDirectoryUrl') ->\
            Iterator[Tuple[BaseDirectoryUrl, BaseDirectoryUrl]]:
        # With GCP, you can't specify a different destination location
        # directory in the GCS bucket than in your source bucket.
        #
        # So let's make sure that if at all possible, we use the same
        # directory in the S3 and GCS bucket, by trying the path from
        # each on the other.
        optimized_temp_first_loc = self._try_swapping_bucket_path(temp_first_loc,
                                                                  temp_second_loc)
        if optimized_temp_first_loc is not None:
            try:
                yield (optimized_temp_first_loc, temp_second_loc)
                return
            finally:
                optimized_temp_first_loc.purge_directory()
        optimized_temp_second_loc = self._try_swapping_bucket_path(temp_second_loc,
                                                                   temp_first_loc)
        if optimized_temp_second_loc is not None:
            try:
                yield (temp_first_loc, optimized_temp_second_loc)
                return
            finally:
                optimized_temp_second_loc.purge_directory()
        logger.warning("Could not match paths between source and destination buckets--"
                       "will not be able to use Google Cloud Platform Data Transfer Service for "
                       "cloud-based copy.")
        yield (temp_first_loc, temp_second_loc)

    @contextmanager
    def optimize_temp_second_location(self,
                                      permanent_first_loc: 'S3DirectoryUrl',
                                      temp_second_loc: 'GCSDirectoryUrl') ->\
            Iterator[BaseDirectoryUrl]:
        # Same as optimize_temp_locations, but this assumes the first
        # location is fixed but the second one can be adjusted.
        optimized_temp_second_loc = self._try_swapping_bucket_path(temp_second_loc,
                                                                   permanent_first_loc)
        if optimized_temp_second_loc is not None:
            try:
                yield optimized_temp_second_loc
            finally:
                optimized_temp_second_loc.purge_directory()
            return
        logger.warning("Could not match paths between source and destination buckets--"
                       "will not be able to use Google Cloud Platform Data Transfer Service for "
                       "cloud-based copy.")
        yield temp_second_loc
