from .s3_base_url import S3BaseUrl
from .awscli import aws_cli
from ..base import BaseDirectoryUrl, BaseFileUrl
from ..filesystem import FilesystemDirectoryUrl
import googleapiclient
import google.auth.credentials
import json
import time
import logging
from typing import Dict, List, TYPE_CHECKING
if TYPE_CHECKING:
    from records_mover.url.gcs.gcs_directory_url import GCSDirectoryUrl
    from googleapiclient.discovery import _TransferJobConfig


logger = logging.getLogger(__name__)


def _wait_for_transfer_job(project_id: str,
                           job_name: str,
                           gcp_credentials: google.auth.credentials.Credentials) -> None:
    # TODO: Pass in correct creds
    storagetransfer = googleapiclient.discovery.build('storagetransfer',
                                                      'v1',
                                                      credentials=gcp_credentials)

    # https://github.com/karthikey-surineni/bigquery-copy-dataset/blob/fb6be5aee9898cba79387a2f95fb54b8b1119a92/src/storage/storage_transfer.py
    filterString = (
        '{{"project_id": "{project_id}", '
        '"job_names": ["{job_name}"]}}'
    ).format(project_id=project_id, job_name=job_name)

    done = False
    logger.info("Awaiting completion of Google Storage Transfer Service job...")
    while not done:
        result = storagetransfer.transferOperations().list(
            name="transferOperations",
            filter=filterString).execute()
        print('Result of transferOperations/list: {}'.format(
            json.dumps(result, indent=4, sort_keys=True)))
        if result != {}:
            done = result['operations'][0]['metadata']['status'] != 'IN_PROGRESS'
        time.sleep(10)
    logger.info("Google Storage Transfer Service job complete.")



def copy_via_gcp_data_transfer(loc: 'S3DirectoryUrl',
                               other_loc: 'GCSDirectoryUrl') -> bool:
    # https://cloud.google.com/storage-transfer/docs/create-manage-transfer-program#python

    if loc.key != other_loc.blob:
        logger.warning("S3 directory does not match GCS directory - "
                       "cannot copy S3 bucket using Google Storage Transfer.  "
                       "Falling back to a slower method of bucket copy.")
        return False

    gcp_credentials = other_loc.credentials
    storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1',
                                                      credentials=gcp_credentials)
    # TODO
    description = "records-mover one-time job"
    # TODO
    project_id = "bluelabs-tools-dev"
    # TODO
    current_day = 10
    # TODO
    current_month = 11
    # TODO
    current_year = 2020
    # TODO
    sink_bucket = "bluelabs-test-recordsmover"
    # TODO
    source_bucket = "bluelabs-scratch"
    # TODO
    aws_creds = loc.aws_creds()
    if aws_creds is None:
        logger.warning("S3 bucket did not provide AWS creds - "
                       "cannot copy S3 bucket using Google Storage Transfer.  "
                       "Falling back to a slower method of bucket copy.")
        return False
    if aws_creds.token is not None:
        logger.warning("S3 bucket is using a temporary access token (MFA creds?) which "
                       "Google Storage Transfer does not support.  Falling back to a slower "
                       "method of bucket copy.")
        return False

    access_key_id = aws_creds.access_key
    # TODO
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
    print('Returned transferJob: {}'.format(
        json.dumps(result, indent=4)))
    job_name = result['name']
    _wait_for_transfer_job(project_id, job_name, gcp_credentials)
    return True

class S3DirectoryUrl(S3BaseUrl, BaseDirectoryUrl):
    def directory_in_this_directory(self, directory_name: str) -> 'S3DirectoryUrl':
        return self._directory(f"{self.url}{directory_name}/")

    def files_in_directory(self) -> List['BaseFileUrl']:
        prefix = self.key
        resp = self.s3_client.list_objects(Bucket=self.bucket, Prefix=prefix, Delimiter='/')
        keys = [item['Key'] for item in resp.get('Contents', [])]
        return [self._key_in_same_bucket(key) for key in keys]

    def directories_in_directory(self) -> List['BaseDirectoryUrl']:
        prefix = self.key
        response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, Delimiter='/')
        prefix_keys = [content['Prefix'] for content in response.get('CommonPrefixes', [])]
        urls = [f"s3://{self.bucket}/{prefix}{key}" for key in prefix_keys]
        return [self._directory(url) for url in urls]

    def purge_directory(self) -> None:
        if not self.is_directory():
            raise ValueError("Not a directory")
        # https://stackoverflow.com/questions/11426560/amazon-s3-boto-how-to-delete-folder
        objects_to_delete = self.s3_resource.meta.client.list_objects(Bucket=self.bucket,
                                                                      Prefix=self.key)
        delete_keys: Dict[str, List[Dict[str, str]]] = {'Objects': []}
        delete_keys['Objects'] =\
            [{'Key': k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]
        if delete_keys['Objects']:
            self.s3_resource.meta.client.delete_objects(Bucket=self.bucket, Delete=delete_keys)

    def copy_to(self, other_loc: BaseDirectoryUrl) -> BaseDirectoryUrl:
        from ..gcs.gcs_directory_url import GCSDirectoryUrl
        if not other_loc.is_directory():
            raise RuntimeError(f"Cannot copy a directory to a file ({other_loc.url})")
        elif isinstance(other_loc, FilesystemDirectoryUrl):
            aws_cli('s3', 'sync', self.url, other_loc.local_file_path)
            return other_loc
        elif isinstance(other_loc, GCSDirectoryUrl):
            if copy_via_gcp_data_transfer(self, other_loc):
                return other_loc
            else:
                return super(S3DirectoryUrl, self).copy_to(other_loc)
        else:
            return super(S3DirectoryUrl, self).copy_to(other_loc)

    def files_matching_prefix(self, prefix: str) -> List[BaseFileUrl]:
        prefix = self.key + prefix
        resp = self.s3_client.list_objects(Bucket=self.bucket, Prefix=prefix, Delimiter='/')
        return [self._key_in_same_bucket(o['Key']) for o in resp.get('Contents', [])]
