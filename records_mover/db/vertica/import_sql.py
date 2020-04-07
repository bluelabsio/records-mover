from records_mover.db.quoting import quote_schema_and_table, quote_value
from typing import Optional


def vertica_import_sql(schema: str,
                       table: str,
                       db_engine,
                       gzip: bool,
                       delimiter: str,
                       trailing_nullcols: bool,
                       record_terminator: str,
                       null_as: str,
                       rejectmax: int,
                       enforcelength: bool,
                       error_tolerance: bool,
                       abort_on_error: bool,
                       load_method: str,
                       no_commit: bool,
                       escape_as: Optional[str] = None,
                       enclosed_by: Optional[str] = None,
                       stream_name: Optional[str] = None,
                       skip: int = 0,
                       rejected_data_table: Optional[str] = None,
                       rejected_data_schema: Optional[str] = None) -> str:

    # https://my.vertica.com/docs/8.1.x/HTML/index.htm#Authoring/SQLReferenceManual/Statements/COPY/COPY.htm
    # https://my.vertica.com/docs/8.1.x/HTML/index.htm#Authoring/SQLReferenceManual/Statements/COPY/Parameters.htm
    # https://my.vertica.com/docs/8.1.x/HTML/index.htm#Authoring/AdministratorsGuide/BulkLoadCOPY/SpecifyingCOPYFROMOptions.htm
    import_sql_template = """
            COPY {schema_and_table}
            FROM STDIN
            {gzip}
            {delimiter_as}
            {trailing_nullcols}
            {null_as}
            {escape}
            {enclosed_by}
            {record_terminator}
            {skip}
            {skip_bytes}
            {trim}
            {rejectmax}
            {rejected_data}
            {exceptions}
            {enforcelength}
            {error_tolerance}
            {abort_on_error}
            {storage}
            {stream_name}
            {no_commit}
            ;
    """

    def quote(value: str) -> str:
        return quote_value(db_engine, value)

    if rejected_data_table is not None and rejected_data_schema is not None:
        rejected_target = quote_schema_and_table(db_engine,
                                                 rejected_data_schema, rejected_data_table)
        rejected_data = f"REJECTED DATA AS TABLE {rejected_target}"
    else:
        rejected_data = ''

    import_sql = import_sql_template.format(
        schema_and_table=quote_schema_and_table(db_engine, schema, table),
        # https://forum.vertica.com/discussion/238556/reading-gzip-files-from-s3-into-vertica
        gzip='GZIP' if gzip else '',
        # The capital E in the next line specifies a string literal
        # that will be backslash-interpreted; the call to python's
        # "repr()" built-in will encode the string with backslashes.
        delimiter_as=f"DELIMITER AS E{repr(delimiter)}",
        trailing_nullcols="TRAILING NULLCOLS" if trailing_nullcols else '',
        null_as=f"NULL AS {quote(null_as)}" if null_as is not None else '',
        escape=f"ESCAPE AS E{repr(escape_as)}" if escape_as is not None else "NO ESCAPE",
        enclosed_by=f"ENCLOSED BY E{repr(enclosed_by)}" if enclosed_by is not None else '',
        record_terminator=f"RECORD TERMINATOR E{repr(record_terminator)}",
        skip=f"SKIP {skip}",
        skip_bytes='',  # only for fixed-width
        trim="",  # only for fixed-width
        rejectmax=f"REJECTMAX {rejectmax}" if rejectmax is not None else '',
        rejected_data=rejected_data,
        exceptions='',   # not yet supported
        enforcelength='ENFORCELENGTH' if enforcelength else '',
        error_tolerance='ERROR TOLERANCE' if error_tolerance else '',
        abort_on_error='ABORT ON ERROR' if abort_on_error else '',
        storage=f"STORAGE {load_method}",
        stream_name=f"STREAM NAME {quote(stream_name)}" if stream_name is not None else '',
        no_commit="NO COMMIT" if no_commit else '',
    )

    return import_sql
