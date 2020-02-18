from enum import Enum


class ExistingTableHandling(Enum):
    DELETE_AND_OVERWRITE = 1
    TRUNCATE_AND_OVERWRITE = 2
    DROP_AND_RECREATE = 3
    APPEND = 4
