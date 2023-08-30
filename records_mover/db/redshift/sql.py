# flake8: noqa

import sqlalchemy
import logging
from typing import Union, Optional
from sqlalchemy.sql import text
from ...check_db_conn_engine import check_db_conn_engine

logger = logging.getLogger(__name__)


select_load_errors = """
        select d.query, substring(d.filename,14,20),
        d.line_number as line,
        substring(d.value,1,16) as value,
        substring(le.err_reason,1,100) as err_reason
        from stl_loaderror_detail d, stl_load_errors le
        where d.query = le.query
        and d.query = :last_copy_id;
        """


def schema_sql_from_admin_views(schema: str,
                                table: str,
                                db: Optional[Union[sqlalchemy.engine.Engine,
                                             sqlalchemy.engine.Connection]],
                                db_conn: Optional[sqlalchemy.engine.Connection] = None,
                                db_engine: Optional[sqlalchemy.engine.Engine] = None)\
        -> Optional[str]:
    # The default behavior in current sqlalchemy driver seems to
    # map "DOUBLE PRECISION" to "DOUBLE_PRECISION", so lean back
    # on admin views for now:
    db, db_conn, db_engine = check_db_conn_engine(db=db, db_conn=db_conn, db_engine=db_engine)
    sql = text("""
SELECT ddl
FROM admin.v_generate_tbl_ddl
WHERE schemaname = :schema_name
AND tablename = :table_name
""")
    out: Optional[str]
    try:
        result = db_conn.execute(sql,  # type: ignore[union-attr]
                                 {'schema_name': schema,
                                  'table_name': table}).fetchall()
        if len(result) == 0:
            out = None
        else:
            # First line is a commented-out drop table, don't need that.
            out = "\n".join(map(lambda r: r.ddl, result[1:]))
    except sqlalchemy.exc.ProgrammingError:
        logger.debug("Error while generating SQL", exc_info=True)
        out = None

    if out is None:
        logger.warning("To be able to save SQL to a records directory, please install "
                       "and grant access to 'admin.v_generate_tbl_ddl' from "
                       "https://github.com/awslabs/amazon-redshift-utils/"
                       "blob/master/src/AdminViews/v_generate_tbl_ddl.sql")
    return out
