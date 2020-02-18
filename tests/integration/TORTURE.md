# Manual torture test for schema handling

This test verifies that different datatypes are copied correctly in
table to table records moves.

## Setup

We'll create a database table with as many of the databases' supported
datatypes as is practical.

Vertica:

```sql
DROP TABLE your_schema.multitest;
CREATE TABLE your_schema.multitest AS
SELECT
      cast(1.2 as numeric) as numeric,
      cast(1.2 as decimal) as decimal,
      cast(1.2 as number) as number,
      cast(1.2 as money) as money,
      cast(1.2 as DOUBLE PRECISION) as double,
      123 AS num,
      cast(123 as integer) AS integer,
      cast(1 as INT) as int,
      cast(1 as BIGINT) as bigint,
      cast(1 as INT8) as int8,
      cast(1 as smallint) as smallint,
      cast(1 as tinyint) as tinyint,
      cast(1.2 as float) as float,
      cast(1.2 as float(23)) as float23,
      cast(1.2 as float8) as float8,
      cast(1.2 as real) as real,
      '123' AS numstr,
      'foo' AS str,
      ',' AS comma,
      '"' AS doublequote,
      '","' AS quotecommaquote,
      E'* SQL unload would generate multiple files (one for each slice/part)\n* Filecat would produce a single data file' AS newlinestr,
      '2000-01-01'::DATE AS date,
      '00:00:00'::TIME AS "time",
      '2000-01-02 12:34:56.789012'::TIMESTAMP AS timestamp,
      '2000-01-02 12:34:56.789012'::DATETIME AS datetime,
      '2000-01-02 12:34:56.789012'::SMALLDATETIME AS smalldatetime,
      '2000-01-02 12:34:56.789012 EDT'::TIMESTAMPTZ as timestamptz,
      cast('2000-01-02 12:34:56.789012 EDT' as timestamp with timezone) as timestamp_with_timezone,
       cast(true as boolean) as boolean,
       cast('a' as char) as CHAR,
       cast('foo' as varchar) AS varchar,
       cast('foo' as long varchar) AS long_varchar;
```

Redshift:

```sql
DROP TABLE your_schema.multitest;
CREATE TABLE your_schema.multitest AS
SELECT
       1.2 as numeric,
       1.2 as decimal,
       cast(1 as smallint) as smallint,
       cast(1 as int2) as int2,
       cast(1 as BIGINT) as bigint,
       cast(1 as INT8) as int8,
       cast(1 as INTEGER) as integer,
       cast(1 as INT) as int,
       cast(1 as INT4) as int4,
       cast(1.2 as real) as real,
       cast(1.2 as float4) as float4,
       cast(1.2 as DOUBLE PRECISION) as double,
       cast(1.2 as float8) as float8,
       cast(1.2 as float) as float,
       true as bool,
       cast(true as boolean) as boolean,
       cast('a' as char) as CHAR,
       cast('a' as character) as CHARACTER,
       cast('a' as nchar) as nchar,
       cast('a' as bpchar) as bpchar,
       123 AS num,
       '123' AS numstr,
       'foo' AS str,
       cast('foo' as varchar) AS varchar,
       cast('foo' as character varying) AS character_varying,
       cast('foo' as nvarchar) AS nvarchar,
       cast('foo' as text) AS text,
       ',' AS comma,
       '"' AS doublequote,
       '","' AS quotecommaquote,
       '* SQL unload would generate multiple files (one for each slice/part)\n* Filecat would produce a single data file' AS newlinestr,
       '2000-01-01'::DATE AS date,
       '00:00:00' AS "time",
       '2000-01-02 12:34:56.789012'::TIMESTAMP AS timestamp,
       cast('2000-01-02 12:34:56.789012' as TIMESTAMP without time zone) AS timestamp_without_time_zone,
       '2000-01-02 12:34:56.789012 US/Eastern'::TIMESTAMPTZ as timestamptz,
       cast('2000-01-02 12:34:56.789012 US/Eastern' as TIMESTAMP with time zone)as timestamp_with_Time_zone;
\d+ your_schema.multitest;
```

## Copy table to another database

e.g. with `mover table2table`

## Verify results in new database

e.g. by using `\d+` in database console.
