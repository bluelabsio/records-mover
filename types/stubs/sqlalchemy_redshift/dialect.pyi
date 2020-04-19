import sqlalchemy as sa
from .commands import AlterTableAppendCommand as AlterTableAppendCommand, Compression as Compression, CopyCommand as CopyCommand, CreateLibraryCommand as CreateLibraryCommand, Encoding as Encoding, Format as Format, UnloadFromSelect as UnloadFromSelect  # noqa
from alembic.ddl import postgresql
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION as DOUBLE_PRECISION  # noqa
from sqlalchemy.dialects.postgresql.base import (PGCompiler,
                                                 PGDDLCompiler,
                                                 PGIdentifierPreparer,
                                                 PGTypeCompiler)
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.types import BIGINT as BIGINT, BOOLEAN as BOOLEAN, CHAR as CHAR, DATE as DATE, DECIMAL as DECIMAL, INTEGER as INTEGER, REAL as REAL, SMALLINT as SMALLINT, TIMESTAMP as TIMESTAMP, VARCHAR as VARCHAR  # noqa
from typing import Any, Optional


class RedshiftImpl(postgresql.PostgresqlImpl):
    __dialect__: str = ...


class TIMESTAMPTZ(sa.dialects.postgresql.TIMESTAMP):
    __visit_name__: str = ...
    def __init__(self) -> None: ...


class RelationKey:
    def __new__(cls, name: Any, schema: Optional[Any] = ..., connection: Optional[Any] = ...): ...
    def unquoted(self): ...


class RedshiftCompiler(PGCompiler):
    def visit_now_func(self, fn: Any, **kw: Any): ...


class RedshiftDDLCompiler(PGDDLCompiler):
    def post_create_table(self, table: Any): ...
    def get_column_specification(self, column: Any, **kwargs: Any): ...


class RedshiftTypeCompiler(PGTypeCompiler):
    def visit_TIMESTAMPTZ(self, type_: Any, **kw: Any): ...


class RedshiftIdentifierPreparer(PGIdentifierPreparer):
    reserved_words: Any = ...


class RedshiftDialect(PGDialect_psycopg2):
    name: str = ...
    max_identifier_length: int = ...
    statement_compiler: Any = ...
    ddl_compiler: Any = ...
    preparer: Any = ...
    type_compiler: Any = ...
    construct_arguments: Any = ...
    def __init__(self, *args: Any, **kw: Any) -> None: ...

    def get_columns(self,
                    connection: Any,
                    table_name: Any,
                    schema: Optional[Any] = ..., **kw: Any):
        ...

    def get_pk_constraint(self,
                          connection: Any,
                          table_name: Any,
                          schema: Optional[Any] = ...,
                          **kw: Any): ...

    def get_foreign_keys(self,
                         connection: Any,
                         table_name: Any,
                         schema: Optional[Any] = ...,
                         **kw: Any):
        ...

    def get_table_names(self, connection: Any, schema: Optional[Any] = ..., **kw: Any): ...
    def get_view_names(self, connection: Any, schema: Optional[Any] = ..., **kw: Any): ...

    def get_view_definition(self,
                            connection: Any,
                            view_name: Any,
                            schema: Optional[Any] = ...,
                            **kw: Any):
        ...

    def get_indexes(self, connection: Any, table_name: Any, schema: Any, **kw: Any): ...

    def get_unique_constraints(self,
                               connection: Any,
                               table_name: Any,
                               schema: Optional[Any] = ...,
                               **kw: Any):
        ...

    def get_table_options(self, connection: Any, table_name: Any, schema: Any, **kw: Any): ...
    def create_connect_args(self, *args: Any, **kwargs: Any): ...
