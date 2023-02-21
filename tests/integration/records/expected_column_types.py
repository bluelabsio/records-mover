# Note that Redshift doesn't support TIME type:
# https://docs.aws.amazon.com/redshift/latest/dg/r_Datetime_types.html
expected_single_database_column_types = {
    'vertica': [
        'INTEGER', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)',
        'VARCHAR(3)', 'VARCHAR(111)', 'DATE', 'TIME',
        'TIMESTAMP', 'TIMESTAMP'
    ],
    'redshift': [
        'INTEGER', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)',
        'VARCHAR(3)', 'VARCHAR(111)', 'DATE', 'VARCHAR(8)',
        'TIMESTAMP', 'TIMESTAMPTZ'
    ],
    'postgresql': [
        'INTEGER', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)',
        'VARCHAR(3)', 'VARCHAR(111)', 'DATE', 'TIME',
        'TIMESTAMP', 'TIMESTAMP'
    ],
    'bigquery': [
        'INTEGER', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)', 'VARCHAR(3)',
        'VARCHAR(111)', 'DATE', 'TIME', 'DATETIME', 'TIMESTAMP'
    ],
    'mysql': [
        'INTEGER', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)', 'VARCHAR(3)',
        'VARCHAR(111)', 'DATE', 'TIME', 'DATETIME', 'DATETIME'
    ],
}

expected_df_loaded_database_column_types = {
    'postgresql': [
        'BIGINT', 'VARCHAR(12)', 'VARCHAR(12)', 'VARCHAR(4)', 'VARCHAR(4)',
        'VARCHAR(12)', 'VARCHAR(444)', 'DATE', 'TIME',
        'TIMESTAMP', 'TIMESTAMP'
    ],
    'mysql': [
        'BIGINT', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)', 'VARCHAR(3)',
        'VARCHAR(111)', 'DATE', 'TIME', 'DATETIME', 'DATETIME'
    ],
    'vertica': [
        'INTEGER', 'VARCHAR(12)', 'VARCHAR(12)', 'VARCHAR(4)', 'VARCHAR(4)',
        'VARCHAR(12)', 'VARCHAR(444)', 'DATE', 'TIME',
        'TIMESTAMP', 'TIMESTAMP'
    ],
    'redshift': [
        'BIGINT', 'VARCHAR(12)', 'VARCHAR(12)', 'VARCHAR(4)', 'VARCHAR(4)',
        'VARCHAR(12)', 'VARCHAR(444)', 'DATE', 'VARCHAR(8)',
        'TIMESTAMP', 'TIMESTAMPTZ'
    ],
    'bigquery': [
        'INTEGER', 'VARCHAR(12)', 'VARCHAR(12)', 'VARCHAR(4)', 'VARCHAR(4)',
        'VARCHAR(12)', 'VARCHAR(444)', 'DATE', 'TIME',
        'DATETIME', 'TIMESTAMP'
    ]
}

expected_table2table_column_types = {
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
    # https://github.com/bluelabsio/records-mover/issues/79
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
    # https://github.com/bluelabsio/records-mover/issues/78
    # https://github.com/bluelabsio/records-mover/issues/79
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
    # bigquery2bigquery:
    #
    # Avro is used in this copy.  According to Google:
    #
    # "Note: There is no logical type that directly corresponds to
    # DATETIME, and BigQuery currently doesn't support any direct
    # conversion from an Avro type into a DATETIME field."
    #
    # https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro
    #
    #
    # bigquery2redshift:
    #
    # Avro is used in this copy.  Redshift doesn't seem to support any
    # of the Avro logicalTypes, meaning that
    # date/time/timestamp/timestamptz all get turned into strings.
    ('postgresql', 'postgresql'): [
        'INTEGER', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)', 'VARCHAR(256)', 'DATE', 'TIME',
        'TIMESTAMP WITHOUT', 'TIMESTAMP'
    ],
    ('postgresql', 'vertica'): [
        'INTEGER', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)', 'VARCHAR(256)', 'DATE', 'TIME',
        'TIMESTAMP', 'TIMESTAMP'
    ],
    ('postgresql', 'redshift'): [
        'INTEGER', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)', 'VARCHAR(256)', 'DATE', 'VARCHAR(8)',
        'TIMESTAMP', 'TIMESTAMPTZ'
    ],
    ('postgresql', 'bigquery'): [
        'INTEGER', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)', 'VARCHAR(256)', 'DATE', 'TIME', 'DATETIME', 'TIMESTAMP'
    ],
    ('redshift', 'vertica'): [
        'INTEGER', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)',
        'VARCHAR(3)', 'VARCHAR(111)', 'DATE', 'VARCHAR(8)',
        'TIMESTAMP', 'TIMESTAMP'
    ],
    ('redshift', 'postgresql'): [
        'INTEGER', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)',
        'VARCHAR(3)', 'VARCHAR(111)', 'DATE', 'VARCHAR(8)',
        'TIMESTAMP', 'TIMESTAMP'
    ],
    ('bigquery', 'redshift'): [
        'BIGINT', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)', 'VARCHAR(256)', 'INTEGER', 'INTEGER',
        'VARCHAR(256)', 'VARCHAR(256)'
    ],
    ('bigquery', 'bigquery'): [
        'INTEGER', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)', 'VARCHAR(256)', 'DATE', 'TIME', 'VARCHAR(256)', 'TIMESTAMP'
    ],
    ('bigquery', 'postgresql'): [
        'BIGINT', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)', 'VARCHAR(256)', 'DATE', 'TIME',
        'TIMESTAMP', 'TIMESTAMP'
    ],
    ('bigquery', 'vertica'): [
        'INTEGER', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)', 'VARCHAR(256)', 'DATE', 'TIME', 'TIMESTAMP', 'TIMESTAMP'
    ],
    ('redshift', 'bigquery'): [
        'INTEGER', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)', 'VARCHAR(3)',
        'VARCHAR(111)', 'DATE', 'VARCHAR(8)', 'TIMESTAMP', 'TIMESTAMP',
    ],
    ('mysql', 'bigquery'): [
        'INTEGER', 'VARCHAR(12)', 'VARCHAR(12)', 'VARCHAR(4)', 'VARCHAR(4)', 'VARCHAR(12)',
        'VARCHAR(444)', 'DATE', 'TIME', 'DATETIME', 'DATETIME',
    ],
    ('redshift', 'mysql'): [
        'INTEGER', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)', 'VARCHAR(3)',
        'VARCHAR(111)', 'DATE', 'VARCHAR(8)', 'DATETIME', 'DATETIME'
    ],
    ('postgresql', 'mysql'): [
        'INTEGER', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)',
        'VARCHAR(256)', 'DATE', 'TIME', 'DATETIME', 'DATETIME'
    ],
    ('bigquery', 'mysql'): [
        'BIGINT', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)', 'VARCHAR(256)',
        'VARCHAR(256)',
        'VARCHAR(256)', 'DATE', 'TIME', 'DATETIME', 'DATETIME'
    ],
    ('mysql', 'postgresql'): [
        'INTEGER', 'VARCHAR(12)', 'VARCHAR(12)', 'VARCHAR(4)', 'VARCHAR(4)', 'VARCHAR(12)',
        'VARCHAR(444)', 'DATE', 'TIME', 'TIMESTAMP',
        'TIMESTAMP'
    ],
    ('mysql', 'redshift'): [
        'INTEGER', 'VARCHAR(12)', 'VARCHAR(12)', 'VARCHAR(4)', 'VARCHAR(4)', 'VARCHAR(12)',
        'VARCHAR(444)', 'DATE', 'VARCHAR(8)', 'TIMESTAMP',
        'TIMESTAMP'
    ],
    ('vertica', 'postgresql'): [
        'BIGINT', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)',
        'VARCHAR(3)', 'VARCHAR(111)', 'DATE', 'TIME',
        'TIMESTAMP', 'TIMESTAMP'
    ],
    ('vertica', 'redshift'): [
        'BIGINT', 'VARCHAR(3)', 'VARCHAR(3)', 'VARCHAR(1)', 'VARCHAR(1)',
        'VARCHAR(3)', 'VARCHAR(111)', 'DATE', 'VARCHAR(8)',
        'TIMESTAMP', 'TIMESTAMPTZ'
    ],
}
