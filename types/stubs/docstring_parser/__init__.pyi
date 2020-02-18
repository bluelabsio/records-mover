from typing import Optional, List


class DocstringMeta:
    args: List[str]
    description: str


class DocstringParam(DocstringMeta):
    args: List[str]
    description: str
    arg_name: str
    type_name: Optional[str]
    is_optional: Optional[bool]
    default: Optional[str]


class Docstring:
    short_description: Optional[str]
    long_description: Optional[str]
    blank_after_short_description: bool
    blank_after_long_description: bool
    meta: List[DocstringMeta]
    params: List[DocstringParam]


# https://github.com/rr-/docstring_parser/blob/master/docstring_parser/parser.py
#
# Note that while the internal annotations there indicate that 'text'
# is str, in practice they do None checks and pass back an empty
# Docstring:
def parse(text: Optional[str]) -> Docstring:
    ...
