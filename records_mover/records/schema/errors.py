class DeserializationError(Exception):
    """Raised for errors encountered while reading in JSON representations
    of a Records Schema
    """
    pass


class UnsupportedSchemaError(DeserializationError):
    """Raised if the version of the Records Schema read in isn't supported
    by this version of Records Mover.  This is usually a sign that you
    are using an older version of Records Mover to try to read a
    directory written by a newer version of Records Mover.
    """
    pass
