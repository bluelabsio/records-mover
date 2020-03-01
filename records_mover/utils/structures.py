from typing import Dict, Any, TypeVar, List, Union


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
