from .exc import NoVersionError as NoVersionError  # noqa
from .handler.base import Handler as Handler
from .util import PrefixFilter as PrefixFilter  # noqa
from collections import namedtuple
from config_resolver.handler.ini import IniHandler as IniHandler  # noqa
from logging import Filter, Logger
from packaging.version import Version
from typing import Any, Dict, Generator, List, Optional, Tuple, Type, NamedTuple

ConfigID = namedtuple("ConfigID", "group app")

LookupMetadata = namedtuple(
    "LookupMetadata", ["active_path", "loaded_files", "config_id", "prefix_filter"]
)


class LookupResult(NamedTuple):
    config: Dict[str, Dict[str, Any]]
    meta: LookupMetadata


FileReadability = namedtuple("FileReadability", "is_readable filename reason version")


def from_string(data: str, handler: Optional[Handler[Any]] = ...) -> LookupResult: ...


def get_config(
    app_name: str,
    group_name: str = ...,
    lookup_options: Optional[Dict[str, Any]] = ...,
    handler: Optional[Type[Handler[Any]]] = ...,
) -> LookupResult: ...


def prefixed_logger(
    config_id: Optional[ConfigID],
) -> Tuple[Logger, Optional[Filter]]: ...


def get_xdg_dirs(config_id: ConfigID) -> List[str]: ...


def get_xdg_home(config_id: ConfigID) -> str: ...


def effective_path(config_id: ConfigID, search_path: str = ...) -> List[str]: ...


def find_files(
    config_id: ConfigID, search_path: Optional[List[str]] = ..., filename: str = ...
) -> Generator[str, None, None]: ...


def effective_filename(config_id: ConfigID, config_filename: str) -> str: ...


def env_name(config_id: ConfigID) -> str: ...


def is_readable(
    config_id: ConfigID,
    filename: str,
    version: Optional[Version] = ...,
    secure: bool = ...,
    handler: Optional[Type[Handler[Any]]] = ...,
) -> FileReadability: ...
