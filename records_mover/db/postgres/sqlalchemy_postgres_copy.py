# This file is vendored from
# https://raw.githubusercontent.com/jmcarp/sqlalchemy-postgres-copy/01ef522e8e46a6961e227069d465b0cb93e42383/postgres_copy/__init__.py
#
# See https://github.com/jmcarp/sqlalchemy-postgres-copy/pull/14 to
# address the issue on why we had to vendor it instead of creating a
# transitive dpeendency.

import six

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Mapper, class_mapper
from sqlalchemy.sql.operators import ColumnOperators
from sqlalchemy.dialects import postgresql

__version__ = '0.5.0'


def copy_to(source, dest, conn, **flags):
    """Export a query or select to a file. For flags, see the PostgreSQL
    documentation at http://www.postgresql.org/docs/9.5/static/sql-copy.html.

    Examples: ::
        select = MyTable.select()
        with open('/path/to/file.tsv', 'w') as fp:
            copy_to(select, fp, conn)


    :param source: SQLAlchemy query or select
    :param dest: Destination file pointer, in write mode
    :param conn: SQLAlchemy connection
    :param **flags: Options passed through to COPY

    If an existing connection is passed to `conn`, it is the caller's
    responsibility to commit and close.
    """
    dialect = postgresql.dialect()
    statement = getattr(source, 'statement', source)
    compiled = statement.compile(dialect=dialect)
    raw_conn, autoclose = raw_connection_from(conn)
    cursor = raw_conn.cursor()
    query = cursor.mogrify(compiled.string, compiled.params).decode()
    formatted_flags = '({})'.format(format_flags(flags)) if flags else ''
    copy = 'COPY ({}) TO STDOUT {}'.format(query, formatted_flags)
    cursor.copy_expert(copy, dest)
    if autoclose:
        raw_conn.close()


def copy_from(source, dest, conn, columns=(), **flags):
    """Import a table from a file. For flags, see the PostgreSQL documentation
    at http://www.postgresql.org/docs/9.5/static/sql-copy.html.

    Examples: ::
        with open('/path/to/file.tsv') as fp:
            copy_from(fp, MyTable, conn)

    :param source: Source file pointer, in read mode
    :param dest: SQLAlchemy model or table
    :param conn: SQLAlchemy connection
    :param columns: Optional tuple of columns
    :param **flags: Options passed through to COPY

    If an existing connection is passed to `conn`, it is the caller's
    responsibility to commit and close.

    The `columns` flag can be set to a tuple of strings to specify the column
    order. Passing `header` alone will not handle out of order columns, it simply tells
    postgres to ignore the first line of `source`.
    """
    tbl = dest.__table__ if is_model(dest) else dest
    raw_conn, autoclose = raw_connection_from(conn)
    cursor = raw_conn.cursor()
    relation = '.'.join('"{}"'.format(part) for part in (tbl.schema, tbl.name) if part)
    formatted_columns = '({})'.format(','.join(columns)) if columns else ''
    formatted_flags = '({})'.format(format_flags(flags)) if flags else ''
    copy = 'COPY {} {} FROM STDIN {}'.format(
        relation,
        formatted_columns,
        formatted_flags,
    )
    cursor.copy_expert(copy, source)
    if autoclose:
        raw_conn.commit()
        raw_conn.close()


def raw_connection_from(conn):
    """Extract a raw_connection and determine if it should be automatically closed.

    Only connections opened by this package will be closed automatically.
    """
    if hasattr(conn, 'cursor'):
        return conn, False
    if hasattr(conn, 'connection'):
        return conn.connection, False
    return conn.raw_connection(), True


def format_flags(flags):
    return ', '.join(
        '{} {}'.format(key.upper(), format_flag(value))
        for key, value in flags.items()
    )


def format_flag(value):
    return (
        six.text_type(value).upper()
        if isinstance(value, bool)
        else repr(value)
    )


def relabel_query(query):
    """Relabel query entities according to mappings defined in the SQLAlchemy
    ORM. Useful when table column names differ from corresponding attribute
    names. See http://docs.sqlalchemy.org/en/latest/orm/mapping_columns.html
    for details.

    :param query: SQLAlchemy query
    """
    return query.with_entities(*query_entities(query))


def query_entities(query):
    return sum(
        [desc_entities(desc) for desc in query.column_descriptions],
        []
    )


def desc_entities(desc):
    expr, name = desc['expr'], desc['name']
    if isinstance(expr, Mapper):
        return mapper_entities(expr)
    elif is_model(expr):
        return mapper_entities(expr.__mapper__)
    elif isinstance(expr, ColumnOperators):
        return [expr.label(name)]
    else:
        raise ValueError('Unrecognized query entity {!r}'.format(expr))


def mapper_entities(mapper):
    model = mapper.class_
    return [
        getattr(model, prop.key).label(prop.key)
        for prop in mapper.column_attrs
    ]


def is_model(class_):
    try:
        class_mapper(class_)
        return True
    except SQLAlchemyError:
        return False
