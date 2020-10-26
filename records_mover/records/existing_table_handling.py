from enum import Enum


class ExistingTableHandling(Enum):
    """Specifies behavior when an existing table with the same name is
    found when loading data into a database.
    """

    DELETE_AND_OVERWRITE = 1
    """Delete data transactionally (typically with a SQL DELETE statement)
    and then add new data to the existing table.  The delete and the
    load will be done in a single transaction if the database allows
    for that, but note that some do not, especially while using the
    most efficient load method.

    """

    TRUNCATE_AND_OVERWRITE = 2
    """Remove data from the existing table without regard for
    transactions, data integrity constraints, triggers or redo logs,
    typically using a SQL TRUNCATE statement.  The specific method
    depends on the database type.  This is typically the fastest way
    to clear a table, but please read your database documentation
    first and understand the consequences.
    """

    DROP_AND_RECREATE = 3
    """Remove the target table entirely, typically with a SQL DROP TABLE
    command."""

    APPEND = 4
    """Leave the target table and current data in place, and add data to
    table.  Note that Records Mover uses the most efficient method of
    loading data into the table, which may not honor triggers and
    integrity constraints.
    """
