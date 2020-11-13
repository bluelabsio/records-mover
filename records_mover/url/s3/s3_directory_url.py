from .s3_base_url import S3BaseUrl
from ..base import BaseDirectoryUrl, BaseFileUrl
import logging
from typing import Dict, List


logger = logging.getLogger(__name__)


class S3DirectoryUrl(S3BaseUrl, BaseDirectoryUrl):
    def directory_in_this_directory(self, directory_name: str) -> 'S3DirectoryUrl':
        return self._directory(f"{self.url}{directory_name}/")

    def directory_in_this_bucket(self, directory_name: str) -> 'S3DirectoryUrl':
        return self._directory(f"s3://{self.bucket}/{directory_name}")

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

    def files_matching_prefix(self, prefix: str) -> List[BaseFileUrl]:
        prefix = self.key + prefix
        resp = self.s3_client.list_objects(Bucket=self.bucket, Prefix=prefix, Delimiter='/')
        return [self._key_in_same_bucket(o['Key']) for o in resp.get('Contents', [])]
