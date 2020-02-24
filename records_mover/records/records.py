from typing import Callable, Optional, Union
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine, Connection
from .processing_instructions import ProcessingInstructions
from .existing_table_handling import ExistingTableHandling
import logging
from .unload_plan import RecordsUnloadPlan
from .records_format import RecordsFormat
from ..db import DBDriver, LoadError
from ..url.resolver import UrlResolver
from .mover import move
from .types import RecordsFormatType
from .sources import RecordsSources
from .targets import RecordsTargets

logger = logging.getLogger(__name__)


class Records:
    def __init__(self,
                 db_driver: Callable[[Union[Engine, Connection]], DBDriver],
                 url_resolver: UrlResolver) -> None:
        self.meta = MetaData()
        self.db_driver = db_driver
        self.url_resolver = url_resolver
        self.move = move
        self.RecordsFormat = RecordsFormat
        self.RecordsUnloadPlan = RecordsUnloadPlan
        self.ProcessingInstructions = ProcessingInstructions
        self.ExistingTableHandling = ExistingTableHandling
        self.LoadError = LoadError
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

    def best_records_format_variant(self,
                                    records_format_type: RecordsFormatType,
                                    db_engine: Engine) -> Optional[str]:
        driver = self.db_driver(db_engine)
        return driver.best_records_format_variant(records_format_type)
