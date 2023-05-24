from typing import Union, IO
import sqlalchemy


def copy_from(source: IO[bytes],
              dest: sqlalchemy.schema.Table,
              conn: sqlalchemy.engine.Connection,
              **flags: object) -> None:
    ...


def copy_to(source: Union[sqlalchemy.sql.expression.Select,
                          sqlalchemy.orm.query.Query],
            dest: IO[bytes],
            conn: sqlalchemy.engine.Connection,
            **flags: object) -> None:
    ...
