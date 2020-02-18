class LoadUnloadError(Exception):
    pass


class NoTemporaryBucketConfiguration(LoadUnloadError):
    pass


class LoadError(LoadUnloadError):
    pass


class UnloadError(LoadUnloadError):
    pass


class S3ExportNotSupported(UnloadError):
    pass


class DatabaseDoesNotSupportS3Export(S3ExportNotSupported):
    pass


class CredsDoNotSupportS3Export(S3ExportNotSupported):
    pass


class CredsDoNotSupportS3Import(LoadError):
    pass
