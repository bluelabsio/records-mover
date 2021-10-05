from records_mover.db.quoting import quote_schema_and_table


class RecordsNumericDatabaseFixture:
    """Create columns with the most diverse set of numeric types
    supported for a given database, and give them standard names
    representing their constraints so we can validate behavior against
    them later on.
    """
    def __init__(self, db_engine, schema_name, table_name):
        self.engine = db_engine
        self.schema_name = schema_name
        self.table_name = table_name

    def bring_up(self):
        if self.engine.name == 'redshift':
            # Redshift supports a number of different numeric types
            create_tables = [f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT 32767::smallint AS int16,
                     2147483647::INTEGER AS int32,
                     9223372036854775807::BIGINT AS int64,
                     1234.56::NUMERIC(6, 2) AS fixed_6_2,
                     12147483647.78::REAL AS float32,
                     19223372036854775807.78::FLOAT AS float64;
"""  # noqa
            ]
        elif self.engine.name == 'vertica':
            # Vertica only supports a few large numeric types
            create_tables = [f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT 9223372036854775807::BIGINT AS int64,
                     1234.56::NUMERIC(6, 2) AS fixed_6_2,
                     19223372036854775807.78::FLOAT AS float64;
"""  # noqa
            ]
        elif self.engine.name == 'bigquery':
            # BigQuery only supports a few large numeric types
            create_tables = [f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT CAST(9223372036854775807 AS INT64) AS int64,
                     CAST(1234.56 AS NUMERIC(6, 2)) AS fixed_6_2,
                     CAST(19223372036854775807.78 AS FLOAT64) AS float64;
"""  # noqa
            ]
        elif self.engine.name == 'postgresql':
            # Postgres supports a number of different numeric types
            create_tables = [f"""
              CREATE TABLE {self.schema_name}.{self.table_name} AS
              SELECT 32767::smallint AS int16,
                     2147483647::INTEGER AS int32,
                     9223372036854775807::BIGINT AS int64,
                     1234.56::NUMERIC(6, 2) AS fixed_6_2,
                     12147483647.78::REAL AS float32,
                     19223372036854775807.78::FLOAT8 AS float64;
"""  # noqa
            ]
        elif self.engine.name == 'mysql':
            # MySQL supports a number of different numeric types
            # https://dev.mysql.com/doc/refman/8.0/en/numeric-types.html
            #
            create_tables = [f"""
              CREATE TABLE {self.schema_name}.{self.table_name} (
                 `int8` TINYINT,
                 `uint8` TINYINT UNSIGNED,
                 `int16` SMALLINT,
                 `uint16` SMALLINT UNSIGNED,
                 `int24` MEDIUMINT,
                 `uint24` MEDIUMINT UNSIGNED,
                 `int32` INT,
                 `uint32` INT UNSIGNED,
                 `int64` BIGINT,
                 `uint64` BIGINT UNSIGNED,
                 `fixed_6_2` DECIMAL(6, 2),
                 `fixed_38_9` DECIMAL(38, 9),
                 `fixed_65_30` DECIMAL(65, 30),
                 `float32` FLOAT,
                 `float64` DOUBLE
              );
""",  # noqa
f"""
              INSERT INTO {self.schema_name}.{self.table_name}
              (
                 `int8`,
                 `uint8`,
                 `int16`,
                 `uint16`,
                 `int24`,
                 `uint24`,
                 `int32`,
                 `uint32`,
                 `int64`,
                 `uint64`,
                 `fixed_6_2`,
                 `fixed_38_9`,
                 `fixed_65_30`,
                 `float32`,
                 `float64`
              )
              VALUES
              (
                  127,
                  128,
                  32767,
                  32768,
                  8388607,
                  8388608,
                  2147483647,
                  2147483648,
                  9223372036854775807,
                  9223372036854775808,
                  1234.56,
                  1234.56,
                  1234.56,
                  12147483647.78,
                  19223372036854775807.78
              );
"""  # noqa
            ]
        else:
            raise NotImplementedError(f"Please teach me how to integration test {self.engine.name}")
        print(f"Creating: {create_tables}")
        for statement in create_tables:
            self.engine.execute(statement)

    def quote_schema_and_table(self, schema, table):
        return quote_schema_and_table(self.engine, schema, table)

    def drop_table_if_exists(self, schema, table):
        sql = f"DROP TABLE IF EXISTS {self.quote_schema_and_table(schema, table)}"
        self.engine.execute(sql)

    def tear_down(self):
        self.drop_table_if_exists(self.schema_name, self.table_name)
