from records_mover.db.quoting import quote_schema_and_table, quote_value
from sqlalchemy.engine import Engine


GIG_IN_BYTES = 1024 * 1024 * 1024


def vertica_export_sql(db_engine: Engine,
                       schema: str,
                       s3_url: str,
                       table: str,
                       delimiter: str,
                       record_terminator: str,
                       to_charset: str,
                       # keep these things halfway digestible in memory
                       chunksize: int=5 * GIG_IN_BYTES) -> str:
    # https://my.vertica.com/docs/8.1.x/HTML/index.htm#Authoring/SQLReferenceManual/Functions/VerticaFunctions/s3export.htm
    template = """
        SELECT S3EXPORT( * USING PARAMETERS {params})
        OVER(PARTITION BEST) FROM {schema_and_table}
    """

    def quote(value: str) -> str:
        return quote_value(db_engine, value)

    params_data = {
        "url": quote(s3_url + 'records.csv'),
        "chunksize": chunksize,
        "to_charset": quote(to_charset),
        "delimiter": quote(delimiter),
        "record_terminator": quote(record_terminator),
    }

    params = ", ".join([f"{key}={value}" for key, value in params_data.items()])
    schema_and_table = quote_schema_and_table(db_engine, schema, table)
    sql = template.format(params=params,
                          schema_and_table=schema_and_table)

    return sql
