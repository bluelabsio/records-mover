from typing import Iterable, Callable, Any, Optional, Dict, List
import docstring_parser
import enum
import inspect
import typing
from typing_inspect import get_origin, is_callable_type, get_args, is_union_type
import collections.abc
import logging
from collections import OrderedDict
from ..mover_types import JsonSchema
from .json_schema_document import JsonSchemaDocument, DefaultValue
from .json_schema_array_document import JsonSchemaArrayDocument
from .json_parameter import JsonParameter


logger = logging.getLogger(__name__)


PythonType = Any


def is_iterable_type(python_type: PythonType) -> bool:
    return get_origin(python_type) in [
        collections.abc.Iterable,
        typing.Iterable,
        typing.List,
        list
    ]


# copied in until a release is cut:
#  https://github.com/ilevkivskyi/typing_inspect/blob/master/typing_inspect.py#L119
def is_optional_type(tp: PythonType) -> bool:
    """Returns `True` if the type is `type(None)`, or is a direct `Union` to `type(None)`, such as `Optional[T]`.  # noqa
    NOTE: this method inspects nested `Union` arguments but not `TypeVar` definitions (`bound`/`constraint`). So it
    will return `False` if
     - `tp` is a `TypeVar` bound, or constrained to, an optional type
     - `tp` is a `Union` to a `TypeVar` bound or constrained to an optional type,
     - `tp` refers to a *nested* `Union` containing an optional type or one of the above.
    Users wishing to check for optionality in types relying on type variables might wish to use this method in
    combination with `get_constraints` and `get_bound`
    """

    if tp is type(None):  # noqa
        return True
    elif is_union_type(tp):
        return any(is_optional_type(tt) for tt in get_args(tp, evaluate=True))
    else:
        return False


def is_impractical_type(python_type: PythonType) -> bool:
    # can't write code and pass it around in JSON!
    return (is_callable_type(python_type) or
            type(python_type) == enum.EnumMeta)


def parse_python_parameter_type(name: str,
                                python_type: PythonType,
                                description: Optional[str],
                                optional: bool,
                                default: DefaultValue) ->\
                Optional[JsonParameter]:
    if python_type == str:
        return JsonParameter(name, JsonSchemaDocument(json_type='string',
                                                      default=default,
                                                      description=description), optional=optional)
    elif python_type == int:
        return JsonParameter(name, JsonSchemaDocument(json_type='integer',
                                                      default=default,
                                                      description=description), optional=optional)
    elif python_type == float:
        return JsonParameter(name, JsonSchemaDocument(json_type='number',
                                                      default=default,
                                                      description=description), optional=optional)
    elif python_type == bool:
        return JsonParameter(name, JsonSchemaDocument(json_type='boolean',
                                                      default=default,
                                                      description=description), optional=optional)
    elif is_optional_type(python_type):
        inner_type = get_args(python_type, evaluate=True)[0]
        if not is_impractical_type(inner_type):
            if default is None:
                default = inspect.Parameter.empty
            return parse_python_parameter_type(name,
                                               inner_type,
                                               description=description,
                                               optional=True,
                                               default=default)
    elif is_iterable_type(python_type):
        inner_type = get_args(python_type, evaluate=True)[0]
        inner_results = parse_python_parameter_type(name, inner_type,
                                                    description=None,
                                                    optional=optional,
                                                    default=inspect.Parameter.empty)

        if inner_results is not None:
            return JsonParameter(inner_results.name,
                                 JsonSchemaArrayDocument(json_type='array',
                                                         items=inner_results.json_schema_document,
                                                         description=description,
                                                         default=default),
                                 optional=inner_results.optional)
    else:
        raise NotImplementedError(f"Teach me how to handle Python type {python_type} "
                                  f"(parameter: {name})")
    return None


def method_signature_to_json_schema(m: Callable[..., Any],
                                    special_handling: Dict[str, List[JsonParameter]],
                                    parameters_to_ignore: Iterable[str]) -> JsonSchema:
    """Creates a JSON schema representing the parameters to a method"""
    signature = inspect.signature(m)
    generated: JsonSchema = {
        'type': 'object',
        'properties': {}
    }

    properties: Dict[str, JsonSchemaDocument] = OrderedDict()
    required: List[str] = []
    method_docs = docstring_parser.parse(m.__doc__)

    def parse_parameter(param_name: str, python_type: PythonType, default: DefaultValue) -> None:
        if param_name in parameters_to_ignore:
            return
        try:
            param_docs = next(param
                              for param in method_docs.params
                              if param.arg_name == param_name)
            description = param_docs.description
        except StopIteration:
            print(f"Could not find docs for {param_name}")
            raise

        if param_name in special_handling:
            for substitute_parameter in special_handling[param_name]:
                properties[substitute_parameter.name] = substitute_parameter.json_schema_document
                if not substitute_parameter.optional:
                    required.append(substitute_parameter.name)
                if properties[substitute_parameter.name].description is None:
                    properties[substitute_parameter.name].description = description
        else:
            optional = default != inspect.Parameter.empty
            results = parse_python_parameter_type(name=param_name,
                                                  python_type=python_type,
                                                  description=description,
                                                  optional=optional,
                                                  default=default)

            if results is not None:
                properties[results.name] = results.json_schema_document
                if not results.optional:
                    required.append(results.name)

    for param_name, parameter in signature.parameters.items():
        python_type = parameter.annotation
        default = parameter.default
        parse_parameter(param_name, python_type, default)
    generated['properties'] = OrderedDict()
    for name, prop in properties.items():
        generated['properties'][name] = prop.to_data()
    generated['required'] = required
    return generated
