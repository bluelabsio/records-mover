# Note that Redshift doesn't support TIME type:
# https://docs.aws.amazon.com/redshift/latest/dg/r_Datetime_types.html
expected_column_types = [
    # Vertica
    [
        'INTEGER', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)',
        'VARCHAR(3)', 'VARCHAR(111)', 'DATE', 'TIME',
        'TIMESTAMP', 'TIMESTAMP'
    ],
    # Redshift
    [
        'INTEGER', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)',
        'VARCHAR(3)', 'VARCHAR(111)', 'DATE', 'VARCHAR(8)',
        'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH TIME ZONE'
    ],
    # Postgres
    [
        'INTEGER', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)',
        'VARCHAR(3)', 'VARCHAR(111)', 'DATE', 'TIME WITHOUT TIME ZONE',
        'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH TIME ZONE'
    ],
    # Postgres when loaded from a dataframe
    [
        'BIGINT', 'VARCHAR(12)', 'VARCHAR(12)', 'VARCHAR(4)', 'VARCHAR(4)',
        'VARCHAR(12)', 'VARCHAR(444)', 'DATE', 'TIME WITHOUT TIME ZONE',
        'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH TIME ZONE'
    ],
    # BigQuery
    [
        "<class 'sqlalchemy.sql.sqltypes.Integer'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.DATE'>",
        "<class 'sqlalchemy.sql.sqltypes.TIME'>",
        "<class 'sqlalchemy.sql.sqltypes.DATETIME'>",
        "<class 'sqlalchemy.sql.sqltypes.TIMESTAMP'>"
    ],
    # MySQL
    [
        'INTEGER(11)', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)', 'VARCHAR(3)',
        'VARCHAR(111)', 'DATE', 'TIME', 'DATETIME(6)', 'DATETIME(6)'
    ],
    # MySQL when loaded from a dataframe
    [
        'BIGINT(20)', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)', 'VARCHAR(3)',
        'VARCHAR(111)', 'DATE', 'TIME', 'DATETIME(6)', 'DATETIME(6)'
    ],
    # Notes on table2table triggered results:
    #
    #
    # redshift2:
    #
    # Since Redshift doesn't have a TIME type, the target will
    # get a string column instead.
    #
    #
    #
    # postgres2, bigquery2:
    #
    # Postgres and BigQuery default to an unlimited length
    # TEXT type, which becomes limited again when copying into
    # databases like Redshift.  We could collect and use
    # statistics to get a better read on this:
    #
    # https://app.asana.com/0/1128138765527694/1152727792219521
    #
    #
    #
    # vertica2, bigquery2:
    #
    # Both Vertica and BigQuery only support a single 64-bit
    # integer type.  When copying to Redshift, this becomes
    # 'BIGINT', as RecordsMover doesn't currently generate
    # types from numeric statistics it pulls from the source
    # tables.
    #
    # https://app.asana.com/0/1128138765527694/1152727792219523
    # https://app.asana.com/0/1128138765527694/1152727792219521
    #
    #
    #
    # redshift2bigquery:
    #
    # According to Google, "DATETIME is not supported for
    # uploading from Parquet" -
    # https://github.com/googleapis/google-cloud-python/issues/9996#issuecomment-572273407
    #
    # So we need to make sure we don't create any DATETIME
    # columns if we're loading from a Parquet file.
    #
    #
    #
    # postgres2postgres
    [
        'INTEGER', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)', 'VARCHAR(256)', 'DATE', 'TIME WITHOUT TIME ZONE',
        'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH TIME ZONE'
    ],
    # postgres2vertica
    [
        'INTEGER', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)', 'VARCHAR(256)', 'DATE', 'TIME',
        'TIMESTAMP', 'TIMESTAMP'
    ],
    # postgres2redshift
    [
        'INTEGER', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)', 'VARCHAR(256)', 'DATE', 'VARCHAR(8)',
        'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH TIME ZONE'
    ],
    # redshift2vertica
    [
        'INTEGER', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)',
        'VARCHAR(3)', 'VARCHAR(111)', 'DATE', 'VARCHAR(8)',
        'TIMESTAMP', 'TIMESTAMP'
    ],
    # bigquery2redshift
    [
        'BIGINT', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)', 'VARCHAR(256)', 'DATE', 'VARCHAR(8)',
        'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH TIME ZONE'
    ],
    # bigquery2postgres
    [
        'BIGINT', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)', 'VARCHAR(256)', 'DATE', 'TIME WITHOUT TIME ZONE',
        'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH TIME ZONE'
    ],
    # bigquery2vertica
    [
        'INTEGER', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)', 'VARCHAR(256)', 'DATE', 'TIME', 'TIMESTAMP', 'TIMESTAMP'
    ],
    # redshift2bigquery
    [
        "<class 'sqlalchemy.sql.sqltypes.Integer'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.DATE'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.TIMESTAMP'>",
        "<class 'sqlalchemy.sql.sqltypes.TIMESTAMP'>",
    ],
    # mysql2bigquery
    [
        "<class 'sqlalchemy.sql.sqltypes.Integer'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.String'>",
        "<class 'sqlalchemy.sql.sqltypes.DATE'>",
        "<class 'sqlalchemy.sql.sqltypes.TIME'>",
        "<class 'sqlalchemy.sql.sqltypes.DATETIME'>",
        "<class 'sqlalchemy.sql.sqltypes.DATETIME'>"
    ],
    # redshift2mysql
    [
        'INTEGER(11)', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)', 'VARCHAR(3)',
        'VARCHAR(111)', 'DATE', 'VARCHAR(8)', 'DATETIME(6)', 'DATETIME(6)'
    ],
    # postgres2mysql
    [
        'INTEGER(11)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)', 'DATE', 'TIME', 'DATETIME(6)', 'DATETIME(6)'
    ],
    # bigquery2mysql
    [
        'BIGINT(20)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)', 'DATE', 'TIME', 'DATETIME(6)', 'DATETIME(6)'
    ],
    # vertica2postgres
    [
        'BIGINT', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)',
        'VARCHAR(3)', 'VARCHAR(111)', 'DATE', 'TIME WITHOUT TIME ZONE',
        'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH TIME ZONE'
    ],
    # vertica2redshift
    [
        'BIGINT', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)',
        'VARCHAR(3)', 'VARCHAR(111)', 'DATE', 'VARCHAR(8)',
        'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH TIME ZONE'
    ],
    # Vertica when loaded from a dataframe
    [
        'INTEGER', 'VARCHAR(12)', 'VARCHAR(12)', 'VARCHAR(4)', 'VARCHAR(4)',
        'VARCHAR(12)', 'VARCHAR(444)', 'DATE', 'TIME',
        'TIMESTAMP', 'TIMESTAMP'
    ],
    # Redshift when loaded from a dataframe
    [
        'BIGINT', 'VARCHAR(12)', 'VARCHAR(12)', 'VARCHAR(4)', 'VARCHAR(4)',
        'VARCHAR(12)', 'VARCHAR(444)', 'DATE', 'VARCHAR(8)',
        'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH TIME ZONE'
    ],
]
