from typing import Union, IO, Optional
import sqlalchemy


def copy_from(source: IO[bytes],
              dest: sqlalchemy.schema.Table,
              engine_or_conn: Optional[Union[sqlalchemy.engine.Engine,
                                             sqlalchemy.engine.Connection]],
              **flags: object) -> None:
    ...


def copy_to(source: Union[sqlalchemy.sql.expression.Select,
                          sqlalchemy.orm.query.Query],
            dest: IO[bytes],
            engine_or_conn: Optional[Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection]],
            **flags: object) -> None:
    ...
