import logging
from .s3_base_url import S3BaseUrl
from ..base import BaseDirectoryUrl, BaseFileUrl
from typing import IO, List, Optional
import threading
from time import sleep
from s3_concat import S3Concat
from smart_open.s3 import open as s3_open


logger = logging.getLogger(__name__)


class S3FileUrl(S3BaseUrl, BaseFileUrl):
    def __str__(self) -> str:
        return self.url

    def filename(self) -> str:
        return self.url[self.url.rfind("/")+1:]

    def directory_in_this_directory(self, directory_name: str) -> BaseDirectoryUrl:
        directory = self.containing_directory()
        return self._directory(f"{directory.url}{directory_name}/")

    def file_in_this_directory(self, filename: str) -> 'S3FileUrl':
        return self._file(self.containing_directory().url + filename)

    def upload_fileobj(self, fileobj: IO[bytes], mode: str = 'wb') -> int:
        # https://github.com/boto/boto3/blob/b2affa81c9b55ebcb9cb3af6e928f4f5acf22cb9/docs/source/guide/s3-uploading-files.rst
        if mode != 'wb':
            # use the single-threaded method that handles all modes
            return super().upload_fileobj(fileobj, mode=mode)

        class _S3ProgressTracker:
            def __init__(self) -> None:
                self.length = 0
                self._lock = threading.Lock()

            def __call__(self, bytes_amount: int) -> None:
                with self._lock:
                    self.length += bytes_amount

        callback = _S3ProgressTracker()

        self.s3_client.upload_fileobj(Fileobj=fileobj,
                                      Bucket=self.bucket,
                                      Key=self.key,
                                      Callback=callback)

        return callback.length

    def open(self, mode: str = "rb") -> IO[bytes]:
        try:
            return s3_open(bucket_id=self.bucket,
                           key_id=self.key,
                           mode=mode,
                           session=self._boto3_session)
        except ValueError as e:
            # Example: ValueError: 'b0KD9AkG7XA/_manifest' does not
            #  exist in the bucket 'vince-scratch', or is forbidden
            #  for access
            #
            # smart-open version: 1.8.x
            if 'does not exist in the bucket' in str(e):
                raise FileNotFoundError(f"{self} not found")
            else:
                raise e
        except OSError as e:
            # Example: OSError: unable to access bucket:
            # 'vince-scratch' key: 'Mv0o7H_YejI/_manifest' version:
            # None error: An error occurred (NoSuchKey) when calling
            # the GetObject operation: The specified key does not
            # exist.
            #
            # smart-open version: 2.0
            if 'NoSuchKey' in str(e):
                raise FileNotFoundError(f"{self} not found")
            else:
                raise e

    def download_fileobj(self, fileobj: IO[bytes]) -> None:
        self.s3_client.download_fileobj(Fileobj=fileobj, Bucket=self.bucket, Key=self.key)

    def store_string(self, contents: str) -> None:
        self.s3_resource.Object(self.bucket, self.key).put(Body=contents)

    def rename_to(self, new: 'BaseFileUrl') -> 'S3FileUrl':
        if not isinstance(new, S3FileUrl):
            raise TypeError(f'Can only rename to same type, not {new}')
        # https://stackoverflow.com/questions/32501995/boto3-s3-renaming-an-object-using-copy-object
        copy_source = {'Bucket': self.bucket, 'Key': self.key}
        self.s3_resource.Object(new.bucket, new.key).copy_from(CopySource=copy_source)
        self.s3_resource.Object(self.bucket, self.key).delete()
        logger.info("Renamed {old_url} to {new_url}".format(old_url=self.url, new_url=new.url))
        return new

    def delete(self) -> None:
        self.s3_resource.Object(self.bucket, self.key).delete()

    def wait_to_exist(self, log_level: int = logging.INFO,
                      ms_between_polls: int = 50) -> None:
        while True:
            try:
                with self.open():
                    return
            except FileNotFoundError:
                logger.log(log_level, f"Waiting for {self.url} to appear...")
                seconds_to_sleep = ms_between_polls / 1000.0
                sleep(seconds_to_sleep)

    def size(self) -> int:
        response = self.s3_client.head_object(Bucket=self.bucket, Key=self.key)
        return response['ContentLength']

    def concatenate_from(self, other_locs: List['BaseFileUrl']) -> Optional[int]:
        if not all([isinstance(loc, S3FileUrl) and loc.bucket == self.bucket
                    for loc in other_locs]):
            logger.warning("Concatenating data locally - this may be slow for large data sets")
            return super().concatenate_from(other_locs)
        job = S3Concat(self.bucket,
                       self.key,
                       session=self._boto3_session,
                       # We want one file the end--S3Concat's other
                       # job in life is concatting small log files
                       # into larger ones, where a minimum file size
                       # is a hint for when to stop combining files
                       # together.
                       min_file_size=None)
        for loc in other_locs:
            assert isinstance(loc, S3FileUrl)  # keep mypy happy
            # Add files, can call multiple times to add files from other directories
            job.add_file(loc.key)

        out = job.concat()
        assert len(out) == 1  # with above arg, this should provide only a single output file

        return None
