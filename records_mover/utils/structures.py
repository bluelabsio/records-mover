from typing import Dict, Any, TypeVar, Callable, List, Union


# Thank you, StackOverflow!
def snake_to_camel(snake_str: str) -> str:
    components = snake_str.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'capitalize' method and join them together.
    return components[0] + "".join(x.capitalize() for x in components[1:])


A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')


def map_keys(f: Callable[[A], B], dict_to_convert: Dict[A, C]) -> Dict[B, C]:
    return {f(name): val for name, val in dict_to_convert.items()}


V = TypeVar('V')


def nest_dict(d: Dict[str, V]) -> Dict[str, Union[V, Dict[str, Any]]]:
    """
    Takes a flat dict that has nested key names like this:

    {'abc': 1, 'foo.bar': 5, 'foo.baz.bing': 'bazzle'}

    ...and turns it into a nested dict that looks like this:

    {'abc': 1, 'foo': {'bar': 5, 'baz': {'bing': 'bazzle'}}}
    """
    def insert_into_dict(msg: Dict[str, Union[V, Dict[str, Any]]],
                         keys: List[str],
                         value: V) -> None:
        if len(keys) == 1:
            msg[keys[0]] = value
        else:
            if keys[0] not in msg:
                msg[keys[0]] = {}
            assert isinstance(msg[keys[0]], dict)
            insert_into_dict(msg[keys[0]], keys[1:], value)  # type: ignore
    msg: Dict[str, Union[V, Dict[str, Any]]] = {}
    for key, val in d.items():
        insert_into_dict(msg, key.split('.'), val)
    return msg
