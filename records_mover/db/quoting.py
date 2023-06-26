from sqlalchemy.engine import Engine, Connection
from typing import Union, Optional
from ..check_db_conn_engine import check_db_conn_engine


def quote_schema_and_table(db: Optional[Union[Connection, Engine]],
                           schema: str,
                           table: str,
                           db_engine: Optional[Engine] = None) -> str:
    """
    Prevent SQL injection when we're not able to use bind
    variables (e.g., passing a table name in SQL).

    http://bobby-tables.com
    http://bobby-tables.com/about

    e.g., for Vertica:

    bobby: Robert'); DROP TABLE Students;--
    quoted_bobby: "Robert'); DROP TABLE Students;--"

    """
    db, _, db_engine = check_db_conn_engine(db=db, db_conn=None, db_engine=db_engine)
    dialect = db_engine.dialect
    return (dialect.preparer(dialect).quote(schema) + '.' +
            dialect.preparer(dialect).quote(table))


def quote_table_only(db: Optional[Union[Connection, Engine]],
                     table: str,
                     db_engine: Optional[Engine] = None) -> str:
    db, _, db_engine = check_db_conn_engine(db=db, db_conn=None, db_engine=db_engine)

    dialect = db_engine.dialect
    return dialect.preparer(dialect).quote(table)


def quote_column_name(db: Optional[Union[Connection, Engine]],
                      column_name: str,
                      db_engine: Optional[Engine] = None) -> str:
    db, _, db_engine = check_db_conn_engine(db=db, db_conn=None, db_engine=db_engine)
    dialect = db_engine.dialect
    return dialect.preparer(dialect).quote(column_name)


def quote_value(db: Optional[Union[Connection, Engine]],
                value: str,
                db_engine: Optional[Engine] = None) -> str:
    """
    Prevent SQL injection on literal string values in places when we're
    not able to use bind variables (e.g., using weird DB-specific
    commands/statements).

    http://bobby-tables.com
    http://bobby-tables.com/about

    e.g., for Vertica:

    bobby: Robert'); DROP TABLE Students;--
    quoted_bobby: 'Robert''); DROP TABLE Students;--'

    """
    db, _, db_engine = check_db_conn_engine(db=db, db_conn=None, db_engine=db_engine)
    dialect = db_engine.dialect
    return dialect.preparer(dialect, initial_quote="'").quote(value)


def quote_user_name(db: Optional[Union[Connection, Engine]],
                    user_name: str,
                    db_engine: Optional[Engine] = None) -> str:
    db, _, db_engine = check_db_conn_engine(db=db, db_conn=None, db_engine=db_engine)
    dialect = db_engine.dialect
    return dialect.preparer(dialect).quote_identifier(user_name)


def quote_group_name(db: Optional[Union[Connection, Engine]],
                     group_name: str,
                     db_engine: Optional[Engine] = None) -> str:
    db, _, db_engine = check_db_conn_engine(db=db, db_conn=None, db_engine=db_engine)
    dialect = db_engine.dialect
    return dialect.preparer(dialect).quote_identifier(group_name)
