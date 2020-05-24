"""Defines hints supported by the job config parser."""
from records_mover.records.delimited.hints import Hints
from ...utils.json_schema import JsonParameter
from typing import Optional


SUPPORTED_HINT_NAMES = [
    'field-delimiter', 'compression', 'escape', 'quoting', 'encoding', 'header-row'
]


# TODO: This class isn't needed anymore if we can just provide Hint class
class SupportedHint:
    """Definition for supported hints"""

    def __init__(self,
                 schema: JsonParameter,
                 target_hint_name: Optional[str] = None):
        self.schema = schema
        self.target_hint_name = target_hint_name or schema.name

    @property
    def config_name(self) -> str:
        return self.schema.name


SUPPORTED_HINTS = [
    SupportedHint(
        JsonParameter(hint_enum.value.hint_name,
                      hint_enum.value.json_schema_document(),
                      optional=True))
    for hint_enum in list(Hints)
]

SUPPORTED_HINT_LOOKUP = {hint.config_name: hint for hint in SUPPORTED_HINTS}
