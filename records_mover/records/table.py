from abc import ABCMeta
from sqlalchemy.engine import Engine, Connection
from typing import Union, Optional, Dict, List
from records_mover.records.existing_table_handling import ExistingTableHandling
from records_mover.db import DBDriver
import logging

logger = logging.getLogger(__name__)


class TargetTableDetails(metaclass=ABCMeta):
    schema_name: str
    table_name: str
    db_engine: Engine
    add_user_perms_for: Optional[Dict[str, List[str]]]
    add_group_perms_for: Optional[Dict[str, List[str]]]
    existing_table_handling: ExistingTableHandling
    drop_and_recreate_on_load_error: bool

    # This should really be a like the above - but mypy gets confused
    # by a function being assigned as a field.
    #
    # Unfortunately it also gets confused by @abstractmethod.
    #
    # https://github.com/python/mypy/issues/5485
    # @abstractmethod
    def db_driver(self, db: Union[Engine, Connection]) -> DBDriver:
        ...
