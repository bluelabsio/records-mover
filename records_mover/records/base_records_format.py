from abc import ABCMeta, abstractmethod
import json
from typing import Mapping, Optional, TYPE_CHECKING
if TYPE_CHECKING:
    from . import RecordsFormatType  # noqa


class BaseRecordsFormat(metaclass=ABCMeta):
    format_type: 'RecordsFormatType'  # noqa

    def json(self) -> str:
        contents = ''
        contents_data = self.config()
        if contents_data is not None:
            contents = json.dumps(contents_data)
        return contents

    def config(self) -> Optional[Mapping[str, object]]:
        return None

    @abstractmethod
    def generate_filename(self, basename: str) -> str:
        pass
