from typing import NamedTuple, Optional, Mapping


SaveResult = NamedTuple('SaveResults', [('output_urls', Mapping[str, str])])
# export_count may be None if database doesn't support accurate counts
UnloadResult = NamedTuple('UnloadResult', [('export_count', Optional[int])])
# import_count may be None if database doesn't support accurate counts
LoadResult = NamedTuple('LoadResult', [('import_count', Optional[int])])

# move_count and output_urls may be empty depending on the nature of
# the move - e.g., a move to a database doesn't currently map to a
# formal URL, and whole-file based moves do not currently count the
# number of records being moved.
MoveResult = NamedTuple('MoveResult',
                        [('move_count', Optional[int]),
                         ('output_urls', Optional[Mapping[str, str]])])
