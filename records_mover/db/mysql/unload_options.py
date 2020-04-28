from typing import TypedDict, Optional, Set
from records_mover.records import DelimitedRecordsFormat


class MySqlUnloadOptions(TypedDict):
    character_set: str  # TODO make this a set of valid options
    field_terminator: str  # default '\t'
    field_enclosed_by: Optional[str] # default ''
    field_optionally_enclosed_by: Optional[str]  # default None
    field_escaped_by: str  # default '\'
    line_starting_by: str  # default ''
    line_terminated_by: str  # default '\n'
    ignore_n_lines: int


def mysql_unload_options(unhandled_hints: Set[str],
                         records_format: DelimitedRecordsFormat,
                         fail_if_cant_handle_hint: bool) -> MySqlUnloadOptions:
    raise NotImplementedError
