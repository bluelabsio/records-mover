#!/usr/bin/env python3

from records_mover.db.quoting import quote_schema_and_table
from records_mover.job_context import get_job_context, BaseJobContext
from datetime import datetime, timedelta
from sqlalchemy import inspect
from typing import Optional
import sys


def is_old(table_name: str) -> bool:
    table_epoch_str = table_name.split('_')[-1]
    table_epoch = int(table_epoch_str)
    table_datetime = datetime.fromtimestamp(table_epoch)
    one_day_ago_datetime = datetime.now() - timedelta(days=1)
    return table_datetime < one_day_ago_datetime


def purge_old_tables(schema_name: str, table_name_prefix: str,
                     db_name: Optional[str] = None) -> None:
    job_context: BaseJobContext = get_job_context()
    if db_name is None:
        db_engine = job_context.get_default_db_engine()
    else:
        db_engine = job_context.get_db_engine(db_name)

    inspector = inspect(db_engine)
    table_names = inspector.get_table_names(schema=schema_name)
    print(f"All tables name in {schema_name}: {table_names}")
    purgable_table_names = [
        table_name
        for table_name in table_names
        if table_name.startswith(f"{table_name_prefix}_")
        and is_old(table_name)
    ]
    print(f"Tables to purge matching {schema_name}.{table_name_prefix}_: {purgable_table_names}")
    for table_name in purgable_table_names:
        sql = f"DROP TABLE {quote_schema_and_table(db_engine, schema_name, table_name)}"
        print(sql)
        db_engine.execute(sql)


if __name__ == '__main__':
    schema_name: str = sys.argv[1]
    table_name_prefix: str = sys.argv[2]
