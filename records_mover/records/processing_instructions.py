from typing import Optional

# An arbitrary 4 mb csv I looked at ran around 100,000 lines.
# Assuming we want to limit our memory usage to, say, 400MB of memory,
# let's limit inference to 1,000,000 lines.
DEFAULT_MAX_SAMPLE_SIZE = 1000000


class ProcessingInstructions:
    def __init__(self,
                 fail_if_dont_understand: bool=True,
                 fail_if_cant_handle_hint: bool=True,
                 fail_if_row_invalid: bool=True,
                 max_inference_rows: Optional[int]=DEFAULT_MAX_SAMPLE_SIZE,
                 max_failure_rows: Optional[int]=None) -> None:
        """Directives on how to handle different situations when processing
        records.  Note that not all vendor mechanisms support this
        level of configurability; when choosing between optimizing for
        fast transfer and ability to comply, Records Mover will favor
        fast transfer.

        :param fail_if_dont_understand: If True, and a part of the RecordsFormat is not understood
           while processing, then immediately fail and raise an exception.  Otherwise, ignore the
           misunderstood instruction (e.g., ignore the hint, assume default variant, etc etc)

        :param fail_if_cant_handle_hint: If True, and for whatever reason (e.g., limited options in
           whatever library/tool/database is being used) a certain hint can't be handled as
           specified, raise an exception.  Otherwise, ignore the hint and use
           implementation-specific different behavior.

        :param fail_if_row_invalid: If True, and a particular row of data in the records file
           cannot be understood by the library, raise an exception.  Otherwise, ignore the row and
           continue and try to load other rows.

        :param max_failure_rows: Sets a tolerance level for number of rows of data in the records
           file that cannot be understood by the library that should be ignored. After reaching
           level, raise an exception.

        :param max_inference_rows: If the schema is not provided and we need it (e.g., we're to
           load the records into a database and there's no existing table), we'll figure it out
           through 'type inference' - looking at a bunch of examples of data and building a
           specific schema that can load those rows.  This can take some time, so this parameter
           controls the maximum number of rows we'll look at.  Higher values will be more likely to
           result in a schema that can be loaded into, but will take longer to load.  If set to
           None, the entire file will be processed.
        """

        self.fail_if_dont_understand = fail_if_dont_understand
        self.fail_if_cant_handle_hint = fail_if_cant_handle_hint
        self.fail_if_row_invalid = fail_if_row_invalid
        self.max_failure_rows = max_failure_rows
        self.max_inference_rows = max_inference_rows
