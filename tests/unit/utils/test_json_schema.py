from records_mover.utils.json_schema import (method_signature_to_json_schema,
                                               JsonParameter, JsonSchemaDocument)
import unittest
from collections import OrderedDict
from typing import Optional, Iterable, List, Callable


class TestJSONSchema(unittest.TestCase):
    maxDiff = None

    def test_method_signature_to_json_schema_no_params(self):
        def m():
            pass

        out = method_signature_to_json_schema(m,
                                              special_handling={},
                                              parameters_to_ignore=[])
        expected = {'type': 'object', 'properties': OrderedDict(), 'required': []}
        self.assertEqual(expected, out)

    def test_method_signature_to_json_schema_with_params(self):
        special_handling = {
            'a_special': [JsonParameter('a_renamed',
                                        JsonSchemaDocument('integer',
                                                           default=1,
                                                           description='a_renamed desc'),
                                        optional=True)]
        }

        def m(a_str: str,
              a_int: int,
              a_float: float,
              a_bool: bool,
              a_optional_str: Optional[str],
              a_iterable_optional_str: Iterable[Optional[str]],
              a_list_list_int: List[List[int]],
              a_ignorable: int,
              a_special: int,
              a_optional_impractical_type: Optional[Callable[[int], int]],
              a_defaulted_string: str="foo") -> None:
            """
            :param a_str: a_str desc
            :param a_int: a_int desc
            :param a_float: a_float desc
            :param a_bool: a_bool desc
            :param a_optional_str: a_optional_str desc
            :param a_iterable_optional_str: a_iterable_optional_str desc
            :param a_list_list_int: a_list_list_int desc
            :param a_ignorable: a_ignorable desc
            :param a_special: a_special desc
            :param a_optional_impractical_type: a_optional_impractical_type desc
            :param a_defaulted_string: a_defaulted_string desc
            """
            pass

        out = method_signature_to_json_schema(m,
                                              special_handling=special_handling,
                                              parameters_to_ignore=['a_ignorable'])
        expected = {
            'properties': OrderedDict([('a_str', {'type': 'string',
                                                  'description': 'a_str desc'}),
                                       ('a_int', {'type': 'integer',
                                                  'description': 'a_int desc'}),
                                       ('a_float', {'type': 'number',
                                                    'description': 'a_float desc'}),
                                       ('a_bool', {'type': 'boolean',
                                                   'description': 'a_bool desc'}),
                                       ('a_optional_str', {'type': 'string',
                                                           'description': 'a_optional_str desc'}),
                                       ('a_iterable_optional_str',
                                        {'items': {'type': 'string'}, 'type': 'array',
                                         'description': 'a_iterable_optional_str desc'}),
                                       ('a_list_list_int',
                                        {
                                            'items': {
                                                'type': 'array',
                                                'items': {
                                                    'type': 'integer'
                                                },
                                            },
                                            'type': 'array',
                                            'description': 'a_list_list_int desc'
                                        }),
                                       ('a_renamed', {'default': 1, 'type': 'integer',
                                                      'description': 'a_renamed desc'}),
                                       ('a_defaulted_string',
                                        {'default': 'foo', 'type': 'string',
                                         'description': 'a_defaulted_string desc'})]),
            'required': ['a_str', 'a_int', 'a_float', 'a_bool', 'a_list_list_int'],
            'type': 'object'
        }
        self.assertEqual(expected, out)
