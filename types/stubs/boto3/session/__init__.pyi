from typing import Any, List, IO, Union, Optional, Dict, Callable
from typing_extensions import Literal
from mypy_extensions import TypedDict
import datetime
from botocore.credentials import Credentials


# One day we'll be able to get deep types into boto3:
#
# https://github.com/alliefitter/boto3_type_annotations/issues/6

class ListObjectRequiredParamsType(TypedDict):
    Bucket: str
    Prefix: str


class ListObjectParamsType(ListObjectRequiredParamsType, total=False):
    ContinuationToken: object


class ListObjectsResponseContentType(TypedDict):
    Key: str
    LastModified: Any
    ETag: str
    Size: int
    StorageClass: str
    Owner: Any


# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
class ListObjectsResponseType(TypedDict):
    IsTruncated: bool
    Contents: List[ListObjectsResponseContentType]
    Name: str
    Prefix: str
    Delimiter: str
    MaxKeys: int
    CommonPrefixes: List[Any]
    EncodingType: str
    KeyCount: int
    ContinuationToken: str
    NextContinuationToken: str
    StartAfter: str


class DeleteObjectsResponseType(TypedDict):
    pass


class GetObjectReponseType(TypedDict):
    Body: IO[bytes]


# https://raw.githubusercontent.com/boto/botostubs/master/botocore-stubs/botocore-stubs/client.pyi
class S3HeadObjectOutput(TypedDict, total=False):
    DeleteMarker: bool
    AcceptRanges: str
    Expiration: str
    Restore: str
    LastModified: datetime.datetime
    ContentLength: int
    ETag: str
    MissingMeta: int
    VersionId: str
    CacheControl: str
    ContentDisposition: str
    ContentEncoding: str
    ContentLanguage: str
    ContentType: str
    Expires: datetime.datetime
    WebsiteRedirectLocation: str
    ServerSideEncryption: str
    Metadata: Dict[str, str]
    SSECustomerAlgorithm: str
    SSECustomerKeyMD5: str
    SSEKMSKeyId: str
    StorageClass: str
    RequestCharged: str
    ReplicationStatus: str
    PartsCount: int
    ObjectLockMode: str
    ObjectLockRetainUntilDate: datetime.datetime
    ObjectLockLegalHoldStatus: str


class S3ClientTypeStub:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client
    def list_objects_v2(self, Bucket: str, Prefix: str,
                        ContinuationToken: Optional[object] = None) -> ListObjectsResponseType:
        ...

    def delete_objects(self, Bucket: str, Delete: Dict[str, Any]) -> DeleteObjectsResponseType:
        ...

    # https://raw.githubusercontent.com/boto/botostubs/master/botocore-stubs/botocore-stubs/client.pyi
    def head_object(self, *,
                    Bucket: str,
                    Key: str,
                    IfMatch: str = ...,
                    IfModifiedSince: datetime.datetime = ...,
                    IfNoneMatch: str = ...,
                    IfUnmodifiedSince: datetime.datetime = ...,
                    Range: str = ...,
                    VersionId: str = ...,
                    SSECustomerAlgorithm: str = ...,
                    SSECustomerKey: str = ...,
                    SSECustomerKeyMD5: str = ...,
                    RequestPayer: str = ...,
                    PartNumber: int = ...) -> S3HeadObjectOutput: ...

    def put_object(self,
                   Bucket: str,
                   Key: str,
                   Body: Any = None,
                   ACL: Optional[str] = None) -> dict: ...

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
    def get_object(self,
                   Bucket: str, Key: str) -> GetObjectReponseType: ...

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_fileobj
    def upload_fileobj(self,
                       Fileobj: IO[bytes], Bucket: str, Key: str, ExtraArgs=None,
                       Callback: Callable[[int], None] = None, Config=None) -> None: ...

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_fileobj
    def download_fileobj(self,
                         Bucket: str, Key: str, Fileobj: IO[bytes], ExtraArgs=None,
                         Callback=None, Config=None) -> None: ...

    def list_objects(self, Bucket: str, Prefix: str,
                     Delimiter: str) -> ListObjectsResponseType:
        ...


class StreamingBodyType:
    _raw_stream: IO[bytes]


class S3ObjectResponseTypeStub(TypedDict):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.get
    Body: StreamingBodyType


class S3ObjectTypeStub:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object
    def get(self, **kwargs: Any) -> S3ObjectResponseTypeStub: ...

    def put(self, Body: str, **kwargs: Any) -> dict: ...

    def copy_from(self, CopySource: Union[str, dict]) -> dict: ...

    def delete(self) -> dict: ...


class S3ResourceTypeStub:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#service-resource
    def Object(self, bucket_name: str, key: str) -> S3ObjectTypeStub: ...
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/clients.html#creating-clients
    meta: Any


class Session:
    region_name: str
    resource: Any

    def __init__(self) -> None:
        ...

    def get_credentials(self) -> Optional[Credentials]:
        ...

    def client(self, resource_type: Literal["s3"]) -> S3ClientTypeStub:
        ...
