from typing import Callable, Union
from sqlalchemy.engine import Engine, Connection
import logging
from ..db import DBDriver
from ..url.resolver import UrlResolver
from .sources import RecordsSources
from .targets import RecordsTargets
from .mover import move

logger = logging.getLogger(__name__)


class Records:
    def __init__(self,
                 db_driver: Callable[[Union[Engine, Connection]], DBDriver],
                 url_resolver: UrlResolver) -> None:
        self.sources = RecordsSources(db_driver=db_driver,
                                      url_resolver=url_resolver)
        self.targets = RecordsTargets(url_resolver=url_resolver,
                                      db_driver=db_driver)
        self.move = move
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
