from typing import Dict, Any, Optional
from . import exceptions  # noqa

JSON = Dict[str, Any]


def validate(instance: JSON,
             schema: JSON,
             cls: Optional[Any] = None,
             *args,
             **kwargs) -> None:
    ...
