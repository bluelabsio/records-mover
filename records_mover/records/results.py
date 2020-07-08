from typing import NamedTuple, Optional, Mapping


class MoveResult(NamedTuple):
    """Represents the result of a move() operation between a records
    source and a records target.

    Note that move_count and output_urls may be empty depending on the
    nature of the move - e.g., a move to a database doesn't currently
    map to a formal URL, and whole-file based moves do not currently
    count the number of records being moved.
    """
    move_count: Optional[int]
    "Number of rows moved (Optional[int])"

    output_urls: Optional[Mapping[str, str]]
    """A dictionary of short string aliases mapping to URLs of the
    resulting data (Optional[Mapping[str, str]])"""
