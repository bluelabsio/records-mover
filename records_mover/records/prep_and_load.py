from typing import Callable, Optional, Type
from records_mover.records.results import MoveResult
from records_mover.db import DBDriver
from records_mover.records.prep import TablePrep
from records_mover.records.table import TargetTableDetails
from records_mover.records.existing_table_handling import ExistingTableHandling
import logging

logger = logging.getLogger(__name__)


def prep_and_load(tbl: TargetTableDetails,
                  prep: TablePrep,
                  schema_sql: str,
                  load: Callable[[DBDriver], Optional[int]],
                  load_exception_type: Type[Exception],
                  reset_before_reload: Callable[[], None] = lambda: None) -> MoveResult:
    logger.info(f"Connecting to database...")
    with tbl.db_engine.begin() as db:
        driver = tbl.db_driver(db)
        prep.prep(schema_sql=schema_sql, driver=driver)
    with tbl.db_engine.begin() as db:
        # This second transaction ensures the table has been created
        # before non-transactional statements like Redshift's COPY
        # take place.  Otherwise you'll get an error like:
        #
        #  Cannot COPY into nonexistent table
        driver = tbl.db_driver(db)
        try:
            import_count = load(driver)
        except load_exception_type:
            if not tbl.drop_and_recreate_on_load_error:
                raise
            reset_before_reload()
            with tbl.db_engine.begin() as db:
                driver = tbl.db_driver(db)
                prep.prep(schema_sql=schema_sql,
                          driver=driver,
                          existing_table_handling=ExistingTableHandling.DROP_AND_RECREATE)
                import_count = load(driver)
        return MoveResult(move_count=import_count, output_urls=None)
