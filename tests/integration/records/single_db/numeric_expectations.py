expected_field_info = {
    'int8': {
        'type': 'integer',
        'constraints': {
            'min': '-128',
            'max': '127'
        }
    },
    'uint8': {
        'type': 'integer',
        'constraints': {
            'min': '0',
            'max': '255'
        }
    },
    'int16': {
        'type': 'integer',
        'constraints': {
            'min': '-32768',
            'max': '32767'
        }
    },
    'uint16': {
        'type': 'integer',
        'constraints': {
            'min': '0',
            'max': '65535'
        }
    },
    'int24': {
        'type': 'integer',
        'constraints': {
            'min': '-8388608',
            'max': '8388607'
        }
    },
    'uint24': {
        'type': 'integer',
        'constraints': {
            'min': '0',
            'max': '16777215'
        }
    },
    'int32': {
        'type': 'integer',
        'constraints': {
            'min': '-2147483648',
            'max': '2147483647'
        }
    },
    'uint32': {
        'type': 'integer',
        'constraints': {
            'min': '0',
            'max': '4294967295'
        }
    },
    'int64': {
        'type': 'integer',
        'constraints': {
            'min': '-9223372036854775808',
            'max': '9223372036854775807'
        }
    },
    'uint64': {
        'type': 'integer',
        'constraints': {
            'min': '0',
            'max': '18446744073709551615'
        }
    },
    'fixed_6_2': {
        'type': 'decimal',
        'constraints': {
            'fixed_precision': 6,
            'fixed_scale': 2
        }
    },
    'fixed_38_9': {
        'type': 'decimal',
        'constraints': {
            'fixed_precision': 38,
            'fixed_scale': 9
        }
    },
    'fixed_65_30': {
        'type': 'decimal',
        'constraints': {
            'fixed_precision': 65,
            'fixed_scale': 30
        }
    },
    'float32': {
        'type': 'decimal',
        'constraints': {
            'fp_total_bits': 32,
            'fp_significand_bits': 23
        }
    },
    'float64': {
        'type': 'decimal',
        'constraints': {
            'fp_total_bits': 64,
            'fp_significand_bits': 53
        }
    }
}


expected_column_types = {
    'redshift': {
        'int8': 'SMALLINT',
        'int16': 'SMALLINT',
        'int32': 'INTEGER',
        'int64': 'BIGINT',
        'ubyte': 'SMALLINT',
        'uint8': 'SMALLINT',
        'uint16': 'INTEGER',
        'uint32': 'BIGINT',
        'uint64': 'NUMERIC(20, 0)',
        'float16': 'REAL',
        'float32': 'REAL',
        'float64': 'DOUBLE PRECISION',
        'float128': 'DOUBLE PRECISION',  # Redshift doesn't support >float64
        'fixed_6_2': 'NUMERIC(6, 2)',
        'fixed_38_9': 'NUMERIC(38, 9)',
        'fixed_100_4': 'DOUBLE PRECISION'  # Redshift doesn't support fixed precision > 38
    },
    'vertica': {
        'int8': 'INTEGER',
        'int16': 'INTEGER',
        'int32': 'INTEGER',
        'int64': 'INTEGER',
        'ubyte': 'INTEGER',
        'uint8': 'INTEGER',
        'uint16': 'INTEGER',
        'uint32': 'INTEGER',
        'uint64': 'NUMERIC(20, 0)',
        'float16': 'FLOAT',
        'float32': 'FLOAT',
        'float64': 'FLOAT',
        'float128': 'FLOAT',  # Vertica doesn't support >float64 - all floats are float64
        'fixed_6_2': 'NUMERIC(6, 2)',
        'fixed_38_9': 'NUMERIC(38, 9)',
        'fixed_100_4': 'NUMERIC(100, 4)'  # Vertica supports precision <= 1024
    },
    'bigquery': {
        'int8': "<class 'sqlalchemy.sql.sqltypes.Integer'>",
        'int16': "<class 'sqlalchemy.sql.sqltypes.Integer'>",
        'int32': "<class 'sqlalchemy.sql.sqltypes.Integer'>",
        'int64': "<class 'sqlalchemy.sql.sqltypes.Integer'>",
        'ubyte': "<class 'sqlalchemy.sql.sqltypes.Integer'>",
        'uint8': "<class 'sqlalchemy.sql.sqltypes.Integer'>",
        'uint16': "<class 'sqlalchemy.sql.sqltypes.Integer'>",
        'uint32': "<class 'sqlalchemy.sql.sqltypes.Integer'>",
        # Numeric has 29=38-9 digits of integer precision, and
        # uint64 has fewer.
        'uint64': "<class 'sqlalchemy.sql.sqltypes.DECIMAL'>",
        'float16': "<class 'sqlalchemy.sql.sqltypes.Float'>",
        'float32': "<class 'sqlalchemy.sql.sqltypes.Float'>",
        'float64': "<class 'sqlalchemy.sql.sqltypes.Float'>",
        # BigQuery doesn't support >float64
        'float128': "<class 'sqlalchemy.sql.sqltypes.Float'>",
        # NUMERIC is precision=38, scale=9, so it fits
        'fixed_6_2': "<class 'sqlalchemy.sql.sqltypes.DECIMAL'>",
        # NUMERIC is precision=38, scale=9, so it fits
        'fixed_38_9': "<class 'sqlalchemy.sql.sqltypes.DECIMAL'>",
        # Escape to floating point, as this is larger than NUMERIC
        'fixed_100_4': "<class 'sqlalchemy.sql.sqltypes.Float'>"
    },
    'postgresql': {
        'int8': 'SMALLINT',
        'int16': 'SMALLINT',
        'int32': 'INTEGER',
        'int64': 'BIGINT',
        'ubyte': 'SMALLINT',
        'uint8': 'SMALLINT',
        'uint16': 'INTEGER',
        'uint32': 'BIGINT',
        'uint64': 'NUMERIC(20, 0)',
        'float16': 'REAL',
        'float32': 'REAL',
        'float64': 'DOUBLE PRECISION',
        'float128': 'DOUBLE PRECISION',  # Postgres doesn't support >float64
        'fixed_6_2': 'NUMERIC(6, 2)',
        'fixed_38_9': 'NUMERIC(38, 9)',
        'fixed_100_4': 'NUMERIC(100, 4)',
    },
    # The numbers after the integer types are display widths - how
    # many spaces to save to render them on output.  Not especially
    # relevant and records-mover just uses the defaults which end up
    # as the below.
    'mysql': {
        'int8': 'TINYINT(4)',
        'int16': 'SMALLINT(6)',
        'int32': 'INTEGER(11)',
        'int64': 'BIGINT(20)',
        'ubyte': 'TINYINT(3) UNSIGNED',
        'uint8': 'TINYINT(3) UNSIGNED',
        'uint16': 'SMALLINT(5) UNSIGNED',
        'uint32': 'INTEGER(10) UNSIGNED',
        'uint64': 'BIGINT(20) UNSIGNED',
        'float16': 'FLOAT',
        'float32': 'FLOAT',
        'float64': 'DOUBLE',
        'float128': 'DOUBLE',  # MySQL doesn't support >float64
        'fixed_6_2': 'DECIMAL(6, 2)',
        'fixed_38_9': 'DECIMAL(38, 9)',
        'fixed_100_4': 'DOUBLE',  # MySQL doesn't support NUMERIC(n,d) where n>65
    },
}
