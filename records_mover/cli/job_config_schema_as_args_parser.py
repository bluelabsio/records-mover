import argparse
from records_mover.utils.structures import nest_dict
import os
import json
from ..mover_types import JsonSchema, JobConfig
from typing import Iterable, Dict, Any, Sequence, List, TypeVar, TYPE_CHECKING
if TYPE_CHECKING:
    from argparse_types import ArgParseArgument

ARG_VALUE_WAS_NONE: object = object()


A = TypeVar('A')


def arguments_output_to_config(arguments_output: Dict[str, A]) -> JobConfig:
    """
    argparse represents unspecified arguents as None, but we want to be
    able to know the difference between 'user told us None' (key set
    in dictionary and set to None) and 'not specified' (key not set in
    dict).
    """
    clean_config = {
        k: None if v == ARG_VALUE_WAS_NONE else v
        for k, v in arguments_output.items()
        if v is not None
    }
    return nest_dict(clean_config)


class JobConfigSchemaAsArgsParser():

    @staticmethod
    def from_description(config_json_schema: JsonSchema,
                         description: str) -> 'JobConfigSchemaAsArgsParser':
        argument_parser =\
            argparse.ArgumentParser(description=description,
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        return JobConfigSchemaAsArgsParser(config_json_schema=config_json_schema,
                                           argument_parser=argument_parser)

    def __init__(self, config_json_schema: JsonSchema,
                 argument_parser: argparse.ArgumentParser) -> None:
        self.config_json_schema = config_json_schema
        self.arg_parser = argument_parser

    def add_to_prefix(self, prefix: str, key: str) -> str:
        """Return the period-separated representation of a nested JSON object.

        This allows nested JSON objects to be specified on the command
        line by representing them as a single parameter.

        (e.g., ./foo --a.b=c maps to {"a": { "b": "c"}}
        """
        if prefix == '':
            return key
        return prefix + '.' + key

    def configure_from_properties(self,
                                  properties: JsonSchema,
                                  required_keys: Iterable[str],
                                  prefix: str = '') -> None:
        for naked_key, raw_value in properties.items():
            if not isinstance(raw_value, dict):
                raise TypeError(f"Did not understand [{raw_value}] in [{properties}]")
            value: Dict[str, object] = raw_value
            key = self.add_to_prefix(prefix, naked_key)
            self.configure_from_single_property(key, value, required_keys, properties)

    def configure_from_single_property(self,
                                       key: str,
                                       value: Dict[str, object],
                                       required_keys: Iterable[str],
                                       properties: JsonSchema) -> None:
        kwargs: ArgParseArgument = {}
        arg_name = self.get_arg_name(key, value)

        if not self.is_key_required(key):
            kwargs['required'] = False
            # When the formatted_key_name differs from key, this will make sure
            # that the arg parser is storing the value in a place that matches
            # the actual schema
            kwargs['dest'] = key

        if 'default' in value:
            kwargs['default'] = value['default']

        if 'description' in value:
            kwargs['help'] = str(value['description'])

        if 'enum' in value:
            self.add_enum_arg(key, arg_name, value, kwargs)
        elif 'type' in value and value['type'] == 'string':
            kwargs['type'] = str
            self.arg_parser.add_argument(arg_name, **kwargs)
        elif 'type' in value and value['type'] == 'integer':
            kwargs['type'] = int
            self.arg_parser.add_argument(arg_name, **kwargs)
        elif 'type' in value and value['type'] == 'array':
            self.add_array_arg(key, arg_name, value, kwargs, properties)
        elif 'type' in value and value['type'] == 'object':
            self.add_object_arg(key, arg_name, value, kwargs, properties)
        elif 'type' in value and value['type'] == 'boolean':
            self.add_bool_arg(key, arg_name, value, kwargs)
        else:
            raise Exception("Did not know how to handle key " + key +
                            " and value " + str(value))

    # For consistency with other args, we map dashes to underscores in the
    # CLI args
    def formatted_key_name(self, key: str) -> str:
        return key.replace('-', '_')

    def is_key_required(self, key: str) -> bool:
        return key in self.required_keys

    def get_arg_name(self, key: str, value: Dict[str, object]) -> str:
        if self.is_key_required(key):
            return key  # why is this not formatted_key_name?

        if value.get('type') == 'boolean' and value.get('default') is True:
            return '--no_' + self.formatted_key_name(key)
        else:
            return "--" + self.formatted_key_name(key)

    def add_enum_arg(self, key, arg_name, value, kwargs):
        enum_values: Iterable[Any] = value['enum']  # type: ignore
        non_none_values: Iterable[Any] = [v for v in enum_values if v is not None]
        kwargs['choices'] = non_none_values
        if None in enum_values:
            # add an arg that maps to 'None' for this key
            no_arg_kwargs: ArgParseArgument = {}
            # if --no_foo is specified, set foo variable to None
            split_key_name = self.formatted_key_name(key).split('.')
            no_arg_arg_name = '--' + '.'.join([*split_key_name[:-1],
                                               'no_' + split_key_name[-1]])
            no_arg_kwargs['action'] = 'store_const'
            no_arg_kwargs['const'] = ARG_VALUE_WAS_NONE
            no_arg_kwargs['dest'] = key
            no_arg_kwargs['required'] = False
            self.arg_parser.add_argument(no_arg_arg_name, **no_arg_kwargs)
        self.arg_parser.add_argument(arg_name, **kwargs)

    def add_array_arg(self, key, arg_name, value, kwargs, props):
        items = value['items']
        if not isinstance(items, dict):
            raise TypeError(f"Did not understand [{items}] in [{props}]")
        item_type = items['type']
        if not isinstance(item_type, str):
            raise TypeError(f"Did not understand [{item_type}] in [{props}]")
        if item_type == 'string':
            kwargs['type'] = str
        elif item_type == 'integer':
            kwargs['type'] = int
        else:
            raise NotImplementedError("Teach me how to handle array item "
                                      f"type of {item_type}")
        kwargs['nargs'] = '*'
        self.arg_parser.add_argument(arg_name, **kwargs)

    def add_object_arg(self, key, arg_name, value, kwargs, props):
        # Recurse and handle nested dicts
        sub_properties = value['properties']
        if not isinstance(sub_properties, dict):
            raise TypeError(f"Did not understand [{sub_properties}] in [{props}]")
        required_subkeys: List[str] = []
        if self.is_key_required(key):
            required_subkeys = [
                self.add_to_prefix(key, partial_subkey)
                for partial_subkey
                in value.get('required', [])  # type: ignore
            ]
        self.configure_from_properties(sub_properties, required_subkeys, prefix=key)

    def add_bool_arg(self, key, arg_name, value, kwargs):
        # https://stackoverflow.com/questions/9183936/boolean-argument-for-script
        if 'default' in value:
            if value['default'] is True:
                # if --no_foo is specified, set foo variable to False
                kwargs['action'] = 'store_false'
            else:
                kwargs['action'] = 'store_true'
        else:
            if self.is_key_required(key):
                raise NotImplementedError("Teach me how to handle a required boolean "
                                          "with no default")
            else:
                # the regular --foo will set this to True:
                kwargs['action'] = 'store_const'
                # store_true sets a default, so we use store_const
                kwargs['const'] = True

                # and we'll add an arg that maps to False for this key:
                false_arg_kwargs: ArgParseArgument = {}
                # if --no_foo is specified, set foo variable to False
                split_key_name = self.formatted_key_name(key).split('.')
                false_arg_arg_name = '--' + '.'.join([*split_key_name[:-1],
                                                      'no_' + split_key_name[-1]])
                false_arg_kwargs['action'] = 'store_const'
                false_arg_kwargs['const'] = False
                false_arg_kwargs['dest'] = key
                false_arg_kwargs['required'] = False
                self.arg_parser.add_argument(false_arg_arg_name, **false_arg_kwargs)
        self.arg_parser.add_argument(arg_name, **kwargs)

    def configure_arg_parser(self) -> None:
        schema = self.config_json_schema
        if schema['type'] == 'object':
            props = schema['properties']
            if not isinstance(props, dict):
                raise TypeError(f"Did not understand {props} in {schema}")
            required_keys_raw = schema.get('required', [])
            if not isinstance(required_keys_raw, list):
                raise TypeError(f"Did not understand {required_keys_raw} in {schema}")
            self.required_keys: List = required_keys_raw
            self.configure_from_properties(props, self.required_keys)
        else:
            raise Exception("Did not know how to parse " +
                            str(self.config_json_schema))

    def parse(self, args: Sequence[str]) -> JobConfig:
        if 'JOB_CONFIG_JSON' in os.environ:
            # https://stackoverflow.com/questions/20199126/reading-json-from-a-file
            with open(os.environ['JOB_CONFIG_JSON']) as json_data:
                return json.load(json_data)
        else:
            if self.config_json_schema is None:
                return {}

            self.configure_arg_parser()
            raw_config = vars(self.arg_parser.parse_args(args))
            return arguments_output_to_config(raw_config)
