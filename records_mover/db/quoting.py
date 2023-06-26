from sqlalchemy.engine import Engine


def quote_schema_and_table(db: Engine,
                           schema: str,
                           table: str) -> str:
    """
    Prevent SQL injection when we're not able to use bind
    variables (e.g., passing a table name in SQL).

    http://bobby-tables.com
    http://bobby-tables.com/about

    e.g., for Vertica:

    bobby: Robert'); DROP TABLE Students;--
    quoted_bobby: "Robert'); DROP TABLE Students;--"

    """
    dialect = db.dialect
    return (dialect.preparer(dialect).quote(schema) + '.' +
            dialect.preparer(dialect).quote(table))


def quote_table_only(db: Engine, table: str) -> str:
    dialect = db.dialect
    return dialect.preparer(dialect).quote(table)


def quote_column_name(db: Engine, column_name: str) -> str:
    dialect = db.dialect
    return dialect.preparer(dialect).quote(column_name)


def quote_value(db: Engine, value: str) -> str:
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
    dialect = db.dialect
    return dialect.preparer(dialect, initial_quote="'").quote(value)


def quote_user_name(db: Engine, user_name: str) -> str:
    dialect = db.dialect
    return dialect.preparer(dialect).quote_identifier(user_name)


def quote_group_name(db: Engine, group_name: str) -> str:
    dialect = db.dialect
    return dialect.preparer(dialect).quote_identifier(group_name)
