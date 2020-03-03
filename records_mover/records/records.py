from sqlalchemy.engine import Engine, Connection
import logging
from ..db import DBDriver
from ..url.resolver import UrlResolver
from .sources import RecordsSources
from .targets import RecordsTargets
from .mover import move
from enum import Enum
from typing import Callable, Union, TYPE_CHECKING
if TYPE_CHECKING:
    from records_mover import Session  # noqa

logger = logging.getLogger(__name__)


class PleaseInfer(Enum):
    # This is a mypy-friendly way of doing a singleton object:
    #
    # https://github.com/python/typing/issues/236
    token = 1


class Records:
    def __init__(self,
                 db_driver: Union[Callable[[Union[Engine, Connection]], DBDriver],
                                  PleaseInfer] = PleaseInfer.token,
                 url_resolver: Union[UrlResolver, PleaseInfer] = PleaseInfer.token,
                 session: Union['Session', PleaseInfer] = PleaseInfer.token) -> None:
        if db_driver is PleaseInfer.token or url_resolver is PleaseInfer.token:
            if session is PleaseInfer.token:
                from records_mover import Session  # noqa

                session = Session()
            if db_driver is PleaseInfer.token:
                db_driver = session.db_driver
            if url_resolver is PleaseInfer.token:
                url_resolver = session.url_resolver
        self.move = move
        self.sources = RecordsSources(db_driver=db_driver,
                                      url_resolver=url_resolver)
        self.targets = RecordsTargets(url_resolver=url_resolver,
                                      db_driver=db_driver)
        """
        To move records from one place to another, you can use the methods on this object.

        Example:

        .. code-block:: python

           records = session.records
           db_engine = session.get_default_db_engine()
           url = 's3://some-bucket/some-directory/'
           source = records.sources.directory_from_url(url=url)
           target = records.targets.table(schema_name='myschema',
                                          table_name='mytable',
                                          db_engine=db_engine)
           results = records.move(source, target)
        """
