import logging
from sqlalchemy import text


logger = logging.getLogger(__name__)

DB_SESSION_TIMEZONE = 'UTC'


def set_session_tz(conn):
    # Vertica (at least) has a concept of a session timezone that
    # affects load tests - if a file with a timestamp without a
    # specific timezone is loaded into a column which stores
    # timezones, the data will be interpreted in and stored as
    # being of the session timezone.
    if conn.engine.name == 'vertica':
        tz = DB_SESSION_TIMEZONE
        conn.execute(text('SET TIME ZONE TO :tz'), {'tz': tz})
        logger.info(f"Set database session timezone to {tz}")
