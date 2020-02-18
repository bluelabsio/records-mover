import logging
from sqlalchemy.schema import CreateTable, MetaData
from sqlalchemy import Table
from typing import Dict, TYPE_CHECKING
from .known_representation import RecordsSchemaKnownRepresentation
from ..field import RecordsSchemaField
if TYPE_CHECKING:
    from ....db import DBDriver  # noqa
    from ..schema import RecordsSchema  # noqa


logger = logging.getLogger(__name__)


def schema_to_schema_sql(records_schema: 'RecordsSchema',
                         driver: 'DBDriver',
                         schema_name: str,
                         table_name: str) -> str:
    meta = MetaData()
    columns = [f.to_sqlalchemy_column(driver) for f in records_schema.fields]
    table = Table(table_name, meta,
                  *columns,
                  schema=schema_name)
    db = None
    if driver is not None:
        db = driver.db
    return str(CreateTable(table, bind=db))


def schema_from_db_table(schema_name: str,
                         table_name: str,
                         driver: 'DBDriver') -> 'RecordsSchema':
    from ..schema import RecordsSchema  # noqa
    logger.info('Pulling table metadata...')
    table = driver.table(schema_name, table_name)
    logger.info(f"Table metadata: {table}")

    fields = []
    origin_representation = RecordsSchemaKnownRepresentation.from_db_driver(driver,
                                                                            schema_name,
                                                                            table_name)
    known_representations: Dict[str, RecordsSchemaKnownRepresentation] = {
        'origin': origin_representation
    }

    for column in table.columns:
        fields.append(RecordsSchemaField.
                      from_sqlalchemy_column(column=column,
                                             driver=driver,
                                             rep_type=origin_representation.type))

    return RecordsSchema(fields=fields,
                         known_representations=known_representations)
