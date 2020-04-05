from typing import Union, IO
import sqlalchemy


def copy_from(source: IO[bytes],
              dest: sqlalchemy.schema.Table,
              engine_or_conn: Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection],
              **flags: object) -> None:
    ...
