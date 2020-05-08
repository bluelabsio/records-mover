import sys
import types


__all__ = [
    '__version__',
    'Session',
    'Records',
    'records',
    'set_stream_logging',
    'move',
]

from . import records
from .version import __version__
from .session import Session
from .records import Records, move
from .logging import set_stream_logging
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .records.sources import RecordsSources
    from .records.targets import RecordsTargets


#
# The following magic allows for importing 'sources' and 'targets'
# with some sensible defaults.
#
# Note: there's a better way to do this once Python 3.6 is EOL
# (2021-12-23)
#
# https://stackoverflow.com/questions/1462986/lazy-module-variables-can-it-be-done
class _Sneaky(types.ModuleType):
    @property
    def _session(self) -> Session:
        if not hasattr(self, '_session_'):
            self._session_ = Session()
            self._session_.set_stream_logging()
        return self._session_

    @property
    def _records(self) -> Records:
        if not hasattr(self, '_records_'):
            self._records_ = self._session.records
        return self._records_

    @property
    def sources(self) -> 'RecordsSources':
        if not hasattr(self, '_sources'):
            self._sources = self._records.sources
        return self._sources

    @property
    def targets(self) -> 'RecordsTargets':
        if not hasattr(self, '_targets'):
            self._targets = self._records.targets
        return self._targets


sys.modules[__name__].__class__ = _Sneaky
