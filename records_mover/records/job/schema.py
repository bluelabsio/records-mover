from ...utils.json_schema import method_signature_to_json_schema, JsonParameter, JsonSchemaDocument
from ..existing_table_handling import ExistingTableHandling
from records_mover.records.delimited.hints import Hints
from records_mover.records.records_types import RECORDS_FORMAT_TYPES
from typing import Any, Dict, List, Callable
from ...mover_types import JsonSchema


HINT_PARAMETERS = [
    JsonParameter(hint_enum.value.hint_name,
                  hint_enum.value.json_schema_document(),
                  optional=True)
    for hint_enum in list(Hints)
]


def method_to_json_schema(method: Callable[..., Any]) -> JsonSchema:
    special_handling: Dict[str, List[JsonParameter]] = {
        'google_cloud_creds': [JsonParameter('gcp_creds_name', JsonSchemaDocument('string'))],
        'db_engine': [JsonParameter('db_name', JsonSchemaDocument('string'))],
        'records_format': ([JsonParameter('variant',
                                          JsonSchemaDocument('string',
                                                             description="Records format variant - "
                                                             "valid for 'delimited' "
                                                             "records format type"),
                                          optional=True),
                            JsonParameter('format',
                                          JsonSchemaDocument('string',
                                                             enum=RECORDS_FORMAT_TYPES,
                                                             description="Records format type.  "
                                                             "Note that 'delimited' includes "
                                                             "CSV/TSV/etc."),
                                          optional=True,)] +
                           HINT_PARAMETERS),
        'initial_hints': HINT_PARAMETERS,
        'existing_table_handling':
        [JsonParameter('existing_table',
                       JsonSchemaDocument('string',
                                          enum=[k.lower()
                                                for k in ExistingTableHandling.__members__],
                                          default='delete_and_overwrite'),
                       optional=True)],
    }

    parameters_to_ignore = [
        'self',
        # maybe in the future we can have people point us to a JSON
        # file for hints...
        'hints',
        # ...and maybe this particular way of passing in dicts:
        'add_user_perms_for',
        'add_group_perms_for',
        # oh yeah, and a way to pass a filename in for those that have
        # hand-crafted a records schema file yet don't have a full
        # records directory...
        'records_schema'
    ]

    return method_signature_to_json_schema(method,
                                           special_handling=special_handling,
                                           parameters_to_ignore=parameters_to_ignore)
