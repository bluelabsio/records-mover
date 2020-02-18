"""Defines exceptions used by records module."""


class RecordsException(Exception):
    """Base class for all records system exceptions."""

    pass


class RecordsFolderNonEmptyException(RecordsException):
    """Raised if trying to write records to a non-empty target directory."""

    pass
