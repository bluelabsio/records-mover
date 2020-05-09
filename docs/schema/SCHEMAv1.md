# Records Schema

## The problem

We'd like to store and transfer data from a wide variety of source
systems (different slightly incompatible SQL databases, CSV files,
Pandas dataframes, Google Sheets spreadsheets, etc).

Some of these sources provide some information about the types and
constraints upon the data inside (e.g, a VARCHAR(12) within a SQL
database).  Other sources (e.g., CSV files) provide no information
whatsoever (representing all data as unlimited-length strings) and any
type information must be determined by direct examination ("CSV type
inference").

The goal of this format is to record whatever information we have
available at the time of capture so that future readers of the data
can do a minimum of type inference on the remaining data for whatever
the target system is.

By doing this, systems like the records mover can interpret that data
and apply it to the target system to create a comfortable landing
place for our records to be stored.

## JSON Schema of Records Schema

See [v1 JSON schema here](./records_schema_v1_schema.json) for
documentation of required fields, valid values, etc.  Descriptions of
key fields are given in the following sections.

## Examples

* [Description of a Redshift table](./redshift_example_1.json)
* [Description of a Pandas DataFrame](./pandas_example_1.json)

To validate these examples, check out this repo and run `make` in this
directory.

## known_representations.*

Because no generic schema format can accommodate the configurations
necessary to fully specify every schema in every system, “known
representations” (or “representations” for short) provide detailed
information about how the field is intended to be represented in
certain systems. In particular, the `origin` representation is a
standard place to include schema information necessary to recreate the
schema precisely as it was on the source system.

Every system that you wish to provide a known representation for needs
to be registered in the top-level `known_representations` field. The
key is the name of the known representation type (e.g., `origin` or
some other short nickname for the type if it is not the `origin` -
e.g., `redshift`).  The value is an object with a `type` field
containing the type of source system (e.g., `sql/redshift`) and other,
type-specific fields that may be useful for reconstructing the schema
in the target system.

## fields

The value of the `fields` top level key is an object where each column
in the source system is described by an object.

### fields.*.type

The `fields.*.type` value may have one of the following values:

* `integer` (numeric type that only includes whole numbers)
* `decimal` (numeric type that includes numbers represented with decimal points)
* `string`
* `boolean`
* `date`
* `time`
* `timetz`
* `datetime`
* `datetimetz`

### fields.*.constraints

Different source systems have different types for storing data with
different constraints. To enable loaders to choose the data type most
appropriate for the data, we allow encoding _constraints_ known in
advance about the data.

As an example of a constraint, if a source system represents an
integer column as a 32-bit unsigned integer, we know that the values
must be between 0 and 4,294,967,295. Or, if a column is defined as
`not null`, we know that a value must be present.

Valid for all types:

* `required`: `true` if the field must be present (`not null` in SQL
  systems); `false` (default) if the value may be missing (boolean)
* `unique`: `true` if a uniqueness constraint is marked in the origin
  system, `false` if it is not, or not specified if we don't know
  whether one is set or not.

Defined constraints for `string`:

* `max_length_bytes`: maximum allowable length of a string in bytes (integer)
* `max_length_chars`: maximum allowable length of a string in characters (integer)

Defined constraints for `integer`:

* `min`: minimum representable numeric value (encoded as a string in JSON to avoid
  JSON numeric limits)
* `max`: maximum representable numeric value (encoded as a string in JSON to avoid
  JSON numeric limits)

Defined constraints for `decimal`:

There are different ways to store real-valued numbers as bytes; these
constraints try to represent the most common.

Note that in source systems that don't put any constraints on the
precision of a column (e.g., a CSV column storing decimal values, or
an explicitly arbitrary precision type), it'd be expected not to see
any of these constraints set.

* `fp_total_bits`: Number of bits used to represent values stored in a
  [floating point format](https://en.wikipedia.org/wiki/Floating-point_arithmetic).
  This number excludes unused padding used for alignment - e.g., a
  numpy long double is 80 bits rather than the 128 total bits of
  memory it takes up in an array.

  This is not provided if the value is not known to be stored in a floating
  point format.
* `fp_significand_bits`: How many of the `fp_total_bits` which are
  dedicated to the
  [significand](https://en.wikipedia.org/wiki/Significand) (Note that
  common values of this are 11 bits when `fp_total_bits` is 16, 23
  bits when `fp_total_bits` is 32, 53 bits when `fp_total_bits` is 64,
  and 64 bits when `fp_totalbits` is 80 (sometimes called a `long
  double`).

  This is not provided if the value is not known to be stored in a floating
  point format.
* `fixed_precision`: Number of significant digits that could be
  represented if the value was stored in a
  [fixed point format](https://en.wikipedia.org/wiki/Fixed-point_arithmetic) in the
  source system.

  This is not provided if the value is not known to be stored in a fixed
  point format.
* `fixed_scale`: How many of those significant digits are devoted to the
  right of the decimal point.

  This is not provided if the value is not known to be stored in a fixed
  point format.

### fields.*.statistics

Statistics provide calculated information about the data extract
(e.g., through type inference).  It may be based on a sample of the
data, and may not reflect future inserts into the source system.

#### Defined statistics

Valid for all types:

* `rows_sampled`: Number of rows sampled to generate these statistics (integer)
* `total_rows`: Total number of rows in the data (integer)

Defined statistics for `string`:

* `max_length_bytes`: maximum observed length of a string in bytes (integer)
* `max_length_chars`: maximum observed length of a string in characters (integer)

Defined statistics for `integer` and `decimal`:

* `min`: minimum observed numeric value (encoded as a string in JSON to avoid
  JSON numeric limits)
* `max`: maximum observed numeric value (encoded as a string in JSON to avoid
  JSON numeric limits)

### fields.*.representations

A field can have a `representation` object which provides a mapping
between the name of a known representation system (registered in
`known_representations` and the value as an object with a `rep_type` field
containing the type of system being represented (e.g., `sql/redshift`)
and other key/values containing the representation of the field
itself. These representation are objects that depend on the specific
type of the representation system.

## Supported representations

### `sql/*` — base type for all SQL databases

`sql` is the base type for all SQL databases and defines a set of
generic attributes, useful in SQL systems.  Different types of SQL
database types are expressed after a `/`--e.g., `sql/redshift`.

#### representations.* attributes

* `col_ddl` (required): the full SQL statement after the column name
* `col_type` (recommended): just the data type from the DDL
* `col_modifiers` (recommended): any column modifiers

#### known_representations.* attributes

* `table_ddl` (required for origin; optional for other
  representations): complete create table statement (string)

### `dataframe/pandas` — Pandas DataFrames

This represents the columnar in-memory datastructure used by the
[Pandas](https://pandas.pydata.org/) library.

Pandas represents tables as 'DataFrames', and columns as 'Series',
with the primary key column being represented by an 'Index'.

#### representations.* attributes

* `pd_df_dtype` (required): object representation of
  [pandas.Series.dtype](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.dtype.html).
  Note that this can be generated by `pandas.io.json.dumps(column_or_series.dtype)`
  (object)
* `pd_df_ftype` (required): string representation of
  [pandas.series.ftype](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.ftype.html)
  (string)
* `pd_df_coltype` (required): Either `"series"` or `"index"` (string)

#### known_representations.* attributes

* `pd_df_dtypes` (required for origin; optional for other
  representations) - object representation of
  [pandas.DataFrame.dtypes](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.dtypes.html).
  Note that this can be generated by `pandas.io.json.dumps(df.dtypes)`
  (object)
* `pd_df_ftypes` (optional; only relevant for older Pandas versions):
  object representation of pandas.DataFrame.ftypes, a legacy field in
  Pandas that was removed in the 1.0 version.  Note that this can be
  generated by `pandas.io.json.dumps(df.ftypes)` (object)

## Proposals

So far, this spec has been used to implement database import and
export via the records specification.  The aspects below haven't been
implemented yet and so are subject to change before being finalized
into this spec:

### fields.*.statistics

#### Defined statistics

* `count`: number of times this field is not null in the data set

### fields.*.encoding

In source data that represents every field as a string (i.e.,
delimited files/CSVs), we need to describe how a given field is
encoded into a string so that we have a chance of decoding it into the
native type on the destination system.

For the `boolean` type, this may be set to :

* `"true/false"`
* `"1/0"`
* `"t/f"`,
* `"yes/no"`

For the `date` type, valid values and semantics are the same as the `dateformat`
[records delimited hint](https://github.com/bluelabsio/knowledge/blob/master/Engineering/Architecture/JobDataExchange/output-design.md#hints)

For the `datetime`/`datetimetz` type, valid values and semantics are the same as
either the `datetimeformat` or `dateformattz`
[hints](https://github.com/bluelabsio/knowledge/blob/master/Engineering/Architecture/JobDataExchange/output-design.md#hints).

For the `time`/`timetz` type, valid values and semantics are the same as the
[`timeonlyformat` hint](https://github.com/bluelabsio/knowledge/blob/master/Engineering/Architecture/JobDataExchange/output-design.md#hints).

Encodings are hints as to the way the source data set is representing
a certain type, when it may be ambiguous.

Notes so far:

* VB: There seems to be a lot of overlap here with hints, most of
  which are directly about representing different types as strings...

### `sql/redshift` — Redshift-specific SQL

#### Record attributes

* `sortkey`
* `sortkeytype`: `"compound"` (default) or `"interleaved"`
* `distkey`
* `diststyle`

Notes so far:

* CW: One thing that the original spec doesn't describe is any
  mechanism for providing database neutral hints as to how to
  sort/distribute/index data. We could just build the inference logic
  in the code that uses the schema. For example, if loading a dataset
  with sql/redshift representation but no sql/vertica representation
  into a vertica database, we can directly use sortkey and distkey for
  the order by/segmented by columns. However, if we go the other
  direction (loading a sql/vertica-represented dataset into redshift),
  we could run into the problem that vertica supports multi-key
  segmentation keys whereas redshift only supports single-key
  distkeys. I guess in this case if the person writing out the
  dataset/schema has a preference, they could just provide a specific
  distkey in a sql/redshift representation. But then what about other
  potential target systems? Should we additionally have a generic
  "suggested" dist/sort key to use if nothing specific is provided?

## Prior Art

* The Avro/Parquet/Arrow family:

  * [Avro](http://avro.apache.org/docs/current/spec.html) - binary
    file format with very flexible schema system. Very common in the
    Hadoop/Kafka world.
  * [Parquet](https://parquet.apache.org/documentation/latest/) -
    columnar data file format that integrates with Avro. Ideal for
    storage of data for processing, e.g., in Spark, and likely to be a
    better long-term storage format for us than CSVs.
  * [Arrow](http://arrow.apache.org/docs/metadata.html) - memory
    layout and IPC format intended to be a common format for operating
    on Parquet data in memory. Used by pyspark, dask and others for
    parquet support and data interchange with java procs.
