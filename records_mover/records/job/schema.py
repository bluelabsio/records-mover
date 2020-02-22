from ...utils.json_schema import method_signature_to_json_schema, JsonParameter, JsonSchemaDocument
from ..existing_table_handling import ExistingTableHandling
from typing import Any, Dict, List, Callable
from ...types import JsonSchema
from .hints import SUPPORTED_HINTS


def method_to_json_schema(method: Callable[..., Any]) -> JsonSchema:
    special_handling: Dict[str, List[JsonParameter]] = {
        'google_cloud_creds': [JsonParameter('gcp_creds_name', JsonSchemaDocument('string'))],
        'db_engine': [JsonParameter('db_name', JsonSchemaDocument('string'))],
        'records_format': ([JsonParameter('variant', JsonSchemaDocument('string'), optional=True)] +
                           [hint.schema for hint in SUPPORTED_HINTS]),
        'initial_hints': [hint.schema for hint in SUPPORTED_HINTS],
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
