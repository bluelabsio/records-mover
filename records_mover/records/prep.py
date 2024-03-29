from typing import Optional
from sqlalchemy.engine import Connection
from records_mover.db.quoting import quote_schema_and_table
from records_mover.records.existing_table_handling import ExistingTableHandling
from records_mover.db import DBDriver
from records_mover.records.table import TargetTableDetails
import logging
from sqlalchemy import text, Table, MetaData, delete
from sqlalchemy.schema import DropTable

logger = logging.getLogger(__name__)


class TablePrep:
    def __init__(self, target_table_details: TargetTableDetails) -> None:
        self.tbl = target_table_details

    def add_permissions(self, conn: Connection, driver: DBDriver) -> None:
        schema_and_table: str = quote_schema_and_table(None,
                                                       self.tbl.schema_name,
                                                       self.tbl.table_name,
                                                       db_engine=driver.db_engine)
        if self.tbl.add_group_perms_for is not None:
            logger.info(f"Adding permissions for {schema_and_table} "
                        f"to group {self.tbl.add_group_perms_for}")
            driver.set_grant_permissions_for_groups(self.tbl.schema_name,
                                                    self.tbl.table_name,
                                                    self.tbl.add_group_perms_for,
                                                    None,
                                                    db_conn=conn)
        if self.tbl.add_user_perms_for is not None:
            logger.info(f"Adding permissions for {schema_and_table} "
                        f"to {self.tbl.add_user_perms_for}")
            driver.\
                set_grant_permissions_for_users(self.tbl.schema_name,
                                                self.tbl.table_name,
                                                self.tbl.add_user_perms_for,
                                                None,
                                                db_conn=conn)

    def create_table(self, schema_sql: str, conn: Connection, driver: DBDriver) -> None:
        logger.info('Creating table...')
        conn.execute(text(schema_sql))
        logger.info(f"Just ran {schema_sql}")
        self.add_permissions(conn, driver)
        logger.info("Table prepped")

    def prep_table_for_load(self,
                            schema_sql: str,
                            existing_table_handling: ExistingTableHandling,
                            driver: DBDriver) -> None:
        logger.info("Looking for existing table..")
        db_engine = driver.db_engine
        db_conn = driver.db_conn

        if driver.has_table(table=self.tbl.table_name, schema=self.tbl.schema_name):
            logger.info("Table already exists.")
            how_to_prep = existing_table_handling
            schema_and_table: str = quote_schema_and_table(None,
                                                           self.tbl.schema_name,
                                                           self.tbl.table_name,
                                                           db_engine=db_engine,)
            if (how_to_prep == ExistingTableHandling.TRUNCATE_AND_OVERWRITE):
                logger.info("Truncating...")
                meta = MetaData()
                table = Table(self.tbl.table_name, meta, schema=self.tbl.schema_name)
                db_conn.execute(table.delete().where(True))
                logger.info("Truncated.")
            elif (how_to_prep == ExistingTableHandling.DELETE_AND_OVERWRITE):
                logger.info("Deleting rows...")
                table_obj = Table(self.tbl.table_name, MetaData(), schema=self.tbl.schema_name)
                db_conn.execute(delete(table_obj).where(True))
                logger.info("Deleted")
            elif (how_to_prep == ExistingTableHandling.DROP_AND_RECREATE):
                with db_engine.connect() as conn:
                    with conn.begin():
                        logger.info(f"The connection object is: {conn}")
                        logger.info("Dropping and recreating...")
                        meta = MetaData()
                        table = Table(self.tbl.table_name, meta, schema=self.tbl.schema_name)
                        drop_table_sql = f"DROP TABLE {schema_and_table}"
                        conn.execute(DropTable(table))  # type: ignore[arg-type]  # noqa: F821
                        logger.info(f"Just ran {drop_table_sql}")
                        self.create_table(schema_sql, conn, driver)
            elif (how_to_prep == ExistingTableHandling.APPEND):
                logger.info("Appending rows...")
            else:
                raise ValueError(f"Don't know how to handle {how_to_prep}")
        else:
            with db_engine.connect() as conn:
                with conn.begin():
                    self.create_table(schema_sql, conn, driver)

    def prep(self,
             schema_sql: str,
             driver: DBDriver,
             existing_table_handling: Optional[ExistingTableHandling] = None) -> None:
        if existing_table_handling is None:
            existing_table_handling = self.tbl.existing_table_handling
        self.prep_table_for_load(schema_sql=schema_sql,
                                 existing_table_handling=existing_table_handling,
                                 driver=driver)
