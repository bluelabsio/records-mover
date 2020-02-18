"""Defines hints supported by the job config parser."""
from ...utils.json_schema import JsonParameter, JsonSchemaDocument
from typing import Optional


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


QUOTING_DESCRIPTION =\
        ('How quotes are applied to individual fields. '
         'all: quote all fields. '
         'minimal: quote only fields that contain ambiguous characters (the '
         'delimiter, the escape character, or a line terminator). '
         'default: never quote fields.')

SUPPORTED_HINTS = [
    SupportedHint(
        JsonParameter('field-delimiter',
                      JsonSchemaDocument('string',
                                         description=('Character used between fields '
                                                      '(default is comma)')),
                      optional=True)),
    SupportedHint(
        JsonParameter('compression',
                      JsonSchemaDocument(['string', 'null'],
                                         enum=['BZIP', 'GZIP', 'LZO', None],
                                         description='Compression type of the file.'),
                      optional=True)),
    SupportedHint(
        JsonParameter('escape',
                      JsonSchemaDocument(['string', 'null'],
                                         enum=['\\', None],
                                         description="Character used to escape strings"),
                      optional=True)),
    SupportedHint(
        JsonParameter('quoting',
                      JsonSchemaDocument(['string', 'null'],
                                         enum=['all', 'minimal', 'nonnumeric', None],
                                         description=QUOTING_DESCRIPTION),
                      optional=True)),
    SupportedHint(
        JsonParameter('encoding',
                      JsonSchemaDocument(['string'],
                                         enum=[
                                             'UTF8', 'UTF16', 'UTF16LE', 'UTF16BE',
                                             'LATIN1', 'CP1252'
                                         ],
                                         description="Text encoding of file"),
                      optional=True)),
]
SUPPORTED_HINT_LOOKUP = {hint.config_name: hint for hint in SUPPORTED_HINTS}
