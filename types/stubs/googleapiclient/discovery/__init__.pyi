from google.auth.credentials import Credentials
from typing_extensions import Literal, TypedDict
from typing import overload, List, Optional


class _ListRequest:
    def execute(self) -> dict:
        ...


class _ObjectsProxy:
    def list(self,
             bucket: str,
             prefix: str,
             delimiter: str) -> _ListRequest:
        ...


class _GCSService:
    def objects(self) -> _ObjectsProxy:
        ...


class _StorageTransferTransferOperationMetadata(TypedDict):
    status: str


class _StorageTransferTransferOperation(TypedDict):
    metadata: _StorageTransferTransferOperationMetadata


class _StorageTransferTransferList(TypedDict):
    operations: List[_StorageTransferTransferOperation]
    ...


class _StorageTransferTransferListOperation:
    def execute(self) -> _StorageTransferTransferList:
        ...


class _StorageTransferTransferOperations:
    def list(self, name: Literal['transferOperations'],
             filter: str) -> _StorageTransferTransferListOperation:
        ...


class _StorageTransferTransferJobResult(TypedDict):
    name: str


class _StorageTransferTransferJob:
    def execute(self) -> _StorageTransferTransferJobResult:
        ...


class _TransferJobScheduleDateConfig(TypedDict):
    day: int
    month: int
    year: int


class _TransferJobScheduleConfig(TypedDict):
    scheduleStartDate: _TransferJobScheduleDateConfig
    scheduleEndDate: _TransferJobScheduleDateConfig


class _TransferJobObjectConditionsConfig(TypedDict):
    includePrefixes: List[str]


class _TransferJobTransferOptionsConfig(TypedDict):
    overwriteObjectsAlreadyExistingInSink: bool
    deleteObjectsUniqueInSink: bool
    deleteObjectsFromSourceAfterTransfer: bool


class _TransferJobGcsDataSinkConfig(TypedDict):
    bucketName: str


class _TransferJobS3DataSourceAwsAccessKeyConfig(TypedDict):
    accessKeyId: str
    secretAccessKey: str


class _TransferJobS3DataSourceConfig(TypedDict):
    bucketName: str
    awsAccessKey: _TransferJobS3DataSourceAwsAccessKeyConfig


class _TransferJobTransferSpecConfig(TypedDict):
    awsS3DataSource: _TransferJobS3DataSourceConfig
    objectConditions: _TransferJobObjectConditionsConfig
    transferOptions: _TransferJobTransferOptionsConfig
    gcsDataSink: _TransferJobGcsDataSinkConfig


class _TransferJobConfig(TypedDict):
    description: str
    status: Literal['ENABLED']
    projectId: str
    schedule: _TransferJobScheduleConfig
    transferSpec: _TransferJobTransferSpecConfig


class _StorageTransferTransferJobs:
    def create(self, body: _TransferJobConfig) -> _StorageTransferTransferJob:
        ...


class _StorageTransfer:
    def transferOperations(self) -> _StorageTransferTransferOperations:
        ...

    def transferJobs(self) -> _StorageTransferTransferJobs:
        ...


@overload
def build(serviceName: Literal['storage'],
          version: Literal['v1'],
          http=None,
          discoveryServiceUrl: str = "...",
          developerKey: Optional[object] = None,
          model: Optional[object] = None,
          requestBuilder: object = "...",
          credentials: Optional[Credentials] = None,
          cache_discovery=True, cache=None,
          client_options: Optional[object] = None,
          adc_cert_path: Optional[str] = None,
          adc_key_path: Optional[str] = None,
          num_retries: int = 1) -> _GCSService:
    ...


@overload
def build(serviceName: Literal['storagetransfer'],
          version: Literal['v1'],
          http=None,
          discoveryServiceUrl: str = "...",
          developerKey: Optional[object] = None,
          model: Optional[object] = None,
          requestBuilder: object = "...",
          credentials: Optional[Credentials] = None,
          cache_discovery=True, cache=None,
          client_options: Optional[object] = None,
          adc_cert_path: Optional[str] = None,
          adc_key_path: Optional[str] = None,
          num_retries: int = 1) -> _StorageTransfer:
    ...
