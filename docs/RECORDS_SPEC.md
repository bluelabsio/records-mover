# Records format

## Goals

The primary goal of this "records format" spec is to define a
self-describing structure for the storage of "rectangular" (record
type) data maximizing interoperability between data platforms.

Examples of "record type" data are a single tab in a spreadsheet, or
an export of a SQL/relational database table.  Records mover assumes
data is stored in columns and rows, and there's a common "scalar" type
for each column of data.

This format aims to represent this data, and:

* Be loaded from and loaded into as quickly and easily as possible via
  analytics databases like BigQuery, Redshift and Vertica, as well as
  data science platforms like Pandas, Spark and Hadoop using vendor
  conventions and tools.
* Build upon existing formats, especially when they have native bulk
  import and export support in commonly used tools.
* Be self-describing, so multiple different tools can process them and
  so conversions can be made between supported formats.  This should
  allow for a flexible tool to export from one tool, do any
  conversions necessary for compatibility, and then import to another
  (a proof of concept can be found in
  [records mover](https://github.com/bluelabsio/records-mover).

## Records directories

A "records directory" is a single-level folder of files that MUST
contain each of the following:

* "data files", containing the actual data itself - this could be
  for example Parquet or "delimited" files (e.g., CSV/TSV).
* a "manifest" pointing to the full list of data files.
* a "records schema file" describing the types of the columns involved
  and information on how that information was obtained.
* a "format file" specifying enough information for consumers to parse
  the data files themselves.  This is particularly useful for parsing
  "records formats" that are ambiguous due to there being multiple
  variants of ways to write them out, like the delimited format.

Additional "metadata files" MAY be written to support additional
features outside the scope of this standard.

Here is an example records directory:

```text
[path]/...
  .../_manifest    <-- manifest file
  .../_format_xyz  <-- records format configuration (how to parse the data files)
  .../_schema      <-- records schema data (scalar type for each column and how it was determined)
  .../_whatever    <-- any other format-relevant metadata files can live here,
  .../xyz1         <-- data files (MUST NOT start with `_`)
  .../xyz2
```

## Data files

One or more data files written MUST be written to `[path]/[any old
file name]`  Note that:

* Data files MUST NOT start with `_` or `.` prefix
* Data files MUST share the same format described in the `_format*`
  file.  Differently formatted data should be written to a different
  records directory.

Valid formats as of this writing--implementations SHOULD support as
many formats as possible for maximum interoperability:

* `avro`
* `parquet`
* `delimited`

See the format file section for details on these formats.

## Manifest

In order to support eventually-consistent storage (e.g., Amazon S3),
some kind of manifest is absolutely essential.  There may be mulitple
data files in a records directory, and in that situation we can't rely
on listing keys to know whether a complete set of output files is yet
present.  To avoid race conditions, code MUST wait for the manifest to
be present before identifying data files, and MUST wait for outputs
that are asserted by the manifest to show up as well (retrying reads
until they succeed) when reading from eventually-consistent storage.

The manifest file:

* MUST be written in `[path]/_manifest`
* MUST follow
  [the Redshift manifest file format](https://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html):
  ```json
  {
    "entries": [
      {"url":"s3://[path]/data_xyz1.csv.bz2","mandatory":true},
      {"url":"s3://[path]/data_xyz2.csv.bz2","mandatory":true}
    ]
  }
  ```
* SHOULD include the additional `content_length` meta field
  describing the length in bytes of the file.  This file is required
  by Redshift for loading ORC/Parquet and for Redshift Spectrum and
  may be required for other scenarios by recordsa format consumers:
  ```json
  {
    "entries": [
      {
        "url": "s3://[path]/data_xyz1.csv.bz2",
        "mandatory": true,
        "meta": { "content_length": 123 }
      },
      {
        "url": "s3://[path]/data_xyz2.csv.bz2",
        "mandatory": true,
        "meta": { "content_length": 456 },
      }
    ]
  }
  ```

`_manifest` MUST be the last file written when creating a records
directory.  Some stores will process writes in order (e.g.,
traditional filesystems).  As a result of this mandate, clients need
only put in a retry loop while trying to fetch the `_manifest` file
from eventually-consistent stores.

While this format is compatible with Redshift's default exported
manifest, Redshift `UNLOAD` writes the manifest to `[prefix]manifest`
(e.g., if prefix is `s3://foo/bar/` then manifest is
`s3://foo/bar/manifest` and data files are `s3://foo/bar/0001_part_00`
etc).  Any operation that has a Redshift `UNLOAD` therefore MUST
rename the manifest file after a successful unload.

## Records schema file

A records schema file MUST be written to the `_schema` file.  More
information about the format of that file can be found
[here](../../RecordsSpecs/SCHEMAv1.md).

## Records format file

A data file format specification file MUST be created with the prefix
`_format`.

These files come in a *simple form* and a *detailed form*.

The *simple form* can be used the `avro` or `parquet` which are
well-defined - in this case, a zero-byte file named `_format_avro` or
`_format_parquet` is written to signify the format.  Clients MUST
ignore any contents of the files to allow for future expansion.

The *detailed form* can be used with formats such as `delimited` which
require more information to be able to parse.  A simple example of the
contents of the `_format_delimited` file:

```json
{
  "type": "delimited",
  "variant": "vertica",
  "hints": {
    "field-delimiter": "\u0001",
    "record-terminator": "\n"
  }
}
```

### delimited format

The delimited format, due to how easy it is to write and how hard it
is to write unambigously, has attracted a number of variants.  To
understand how they differ, this spec defines different `hints` as to
how different situations should be handled in generation or parsing of
the files.

#### hints

Unless otherwise specified, if an implementation does not understand
or has not implemented configuration for a hint, it MAY proceed and
ignore it.  It MUST emit a diagnostic in such a case.  It also MAY
error out execution rather than proceed (e.g., depending on
configuration provided to it outside of this spec).

* `header-row`: Valid values: `true`, `false`

  True if a header row is provided in the delimited files.

  Default value is `false`

* `field-delimiter`: Example value: `","` (CSV == "comma separated
  values"), but any single character can be provided.

  Default value is `","`.

* `compression`: Valid values: `"GZIP"`, `"BZIP"`, `"LZO"`, `null`

  What format the data files are compressed in. `null` denotes no compression.

  Default value is `"GZIP"`

* `record-terminator`: Example values: `"\n"`(unix-style), `"\r\n"`
  (dos-style), `"\r"` (Mac-style)].  `null` implies that this is not
  known for the file--implementations MAY either use this information
  to sniff the file for the correct value--if they do not, they SHOULD
  use the default for the particular delimited format (see below).

  Default value is `"\n"`

* `quoting`: Valid values: `"all"`, `"minimal"`, `"nonnumeric"`, `null`

  See
  [Python's CSV library](https://docs.python.org/3/library/csv.html#csv.QUOTE_ALL)
  for details; we rely on their semantics.

  Default value is `null`.

* `quotechar`: Valid values `"\"".

  See
  [Python's CSV library](https://docs.python.org/3/library/csv.html#csv.Dialect.quotechar)
  for details; we rely on their semantics.

  Default value is `"\""`

* `doublequote`: Valid values `True` or `False`.

  See
  [Python's CSV library](https://docs.python.org/3/library/csv.html#csv.Dialect.doublequote)
  for details; we rely on their semantics.

  Default value is `False`.

* `escape`: Valid values: `"\\"`, `null`

  `"\\"`: Use backslash as an escape character.  See
  [Redshift](https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-escape)
  documentation for more detail.

  Default value is `null`.

* `encoding`: Valid values: `"UTF8"`, `"UTF16"`, `"UTF16LE"`,
  `"UTF16BE"`, `"UTF16BOM"`, `"UTF8BOM"`, `"LATIN1"`, `"CP1252"`

  Default value is `"UTF8"`

* `dateformat`: Valid values: `"YYYY-MM-DD"`, `"MM-DD-YYYY"`,
  `"DD-MM-YYYY"`, `"MM/DD/YY"`.

  See
  [Redshift docs](https://docs.aws.amazon.com/redshift/latest/dg/r_DATEFORMAT_and_TIMEFORMAT_strings.html)
  for more information.

  Default value is `"YYYY-MM-DD"`

* `timeonlyformat`: Valid values: `"HH12:MI AM"`  (e.g., `"1:00 PM"`),
                    `"HH24:MI:SS"` (e.g., "13:00:00")

  See
  [Redshift docs](https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-timeformat)
  for more information.

  Default value is `"HH24:MI"`.

* `datetimeformattz`: Valid values: `"YYYY-MM-DD HH:MI:SSOF"`,
  `"YYYY-MM-DD HH:MI:SS"`, `"YYYY-MM-DD HH24:MI:SSOF"`,
  `"YYYY-MM-DD HH24:MI:SSOF"`, `"MM/DD/YY HH24:MI"`.  See
  [Redshift docs](https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-timeformat)
  for more information (note that `HH:` is equivalent to `HH24:` and
  that if you don't provide an offset (`OF`), times are assumed to be
  in UTC).

  Default value is `"YYYY-MM-DD HH:MI:SSOF"`.

* `datetimeformat`: Valid values: `"YYYY-MM-DD HH24:MI:SS"`,
  `"YYYY-MM-DD HH:MI:SS"`, `"YYYY-MM-DD HH12:MI AM"`, `"MM/DD/YY HH24:MI"`.
  See
  [Redshift docs](https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-timeformat)
  for more information (note that `HH:` is equivalent to `HH24:` and
  that if you don't provide an offset (`OF`), times are assumed to be
  in UTC).

  Default value is `"YYYY-MM-DD HH:MI:SS"`.

### Variants

#### `dumb` variant

This pseudo-standard is ambiguous, but is the simplest to write out.
It represents the output of many hand-crafted CSV file generators.

Default hints (may be overridden):

* `field-delimiter`: `","`
* `record-terminator`: `"\n"`
* `quoting`: `null`
* `doublequote`: `False`
* `dateformat`: `null`

Given there is no provision for handling the character `,` in a cell,
this format is of course prone to rejected rows.

Note: A future revision of this spec might define heuristics that
could be applied to parse many of these rows.

#### `csv` variant

This pseudo-standard is defined by the best effort towards what
spreadsheets like Excel and Google Sheets imports/exports.  Example:

```csv
foo,",","""","* SQL unload would generate multiple files (one for each slice/part)
* Filecat would produce a single data file",1/1/17,1:00 PM,1/1/17 13:00
```

Default hints (may be overridden):

* `field-delimiter`: `","`
* `quoting`: `minimal`
* `doublequote`: `True`
* `quotechar`: `"\""`
* `timeonlyformat`: `"HH24:MI:SS"`
* `dateformat`: `"MM/DD/YY"`
* `datetimeformat`: `"MM/DD/YY HH24:MI"`
* `datetimeformattz`: `"MM/DD/YY HH24:MI"`
* `header-row`: `True`

#### `bluelabs` variant

This is the best shot at a multi-database-capable variant we have so
far.

While Redshift can both import and export this flawlessly, Vertica can
ingest this, but cannot produce it.  If you need to export from
Vertica, use the `vertica` variant below.

Default hints (may be overridden):

* `field-delimiter`: `","`
* `escape`: `"\\"`
* `quoting`: `null`
* `doublequote`: `False`
* `dateformat`: `"YYYY-MM-DD"`
* `datetimeformat`: `"YYYY-MM-DD HH24:MI:SS"`

#### `vertica` variant

This represents the a format which is less likely to be ambiguous
and can be imported and exported by Vertica.  Unfortunately this
format can't be imported by Redshift without first converting it to
another format; it doesn't support alternate record terminators.

* `field-delimiter`: `"\001"`
* `record-terminator`: `"\002"`
* `quoting`: `null`
* `doublequote`: `False`
* `escape`: `null`
* `compression`: `null`

Note: `\001` and `\002` must be removed from output if this is to be
completely unambiguous (meaning no arbitrary binary columns).

#### `bigquery` variant

Google BigQuery as of 2019-06-13 has some limitations on import:

* It supports escaping like Google Sheets does, by double quoting
  things and representing " in a string as "", like the CSV variant.

* It does not support escaping strings with backslashes, so using the
  'bluelabs' variant is right out.

* So, CSV would be an obvious favorite variant for loading, right?
  Well, not so fast. The CSV variant writes dates as MM/DD/YY (because
  that's how Excel writes them), and BigQuery doesn't support reading
  them that way.

* BigQuery supports exactly one format of ingesting timestamps with
  timezones (what they call 'TIMESTAMP' they call timestamps without
  timezones 'DATETIME'.

  That format they accept is ISO 8601, which sounds all nice and
  standardy. Usable timestamps look like 2000-01-02
  16:34:56.789012+00:00.

  Cool cool. The only issue is that Python's strftime doesn't
  actually provide a way to add the ':' in the timezone offset. The
  only timezone offset code, %z, does not provide the colon. Other
  implementations (GNU libc) offers the %:z option, but that doesn't
  exist in Python and thus in Pandas.

  Most databases will parse either the version with colon or without
  just fine.

  Not BigQuery.

  So, we now allow a hint value to express a timestamp with
  timezone...without an actual timezone. The implication is that the
  value is in UTC (which is exactly what Python/Pandas' strftime()
  converts it to in practice). Fortunately, BigQuery doesn't store the
  offset you pass in regardless and just converts the date to UTC when
  storing, so no information lost that wasn't going to get lost
  anyway.

  So we prefer writing without a timezone offset to avoid the
  implementation issue above.

Default hints (may be overridden):

* `field-delimiter`: `","`
* `quoting`: `"minimal"`
* `doublequote`: `True`
* `quotechar`: `"\""`
* `header-row`: `True`
* `datetimeformat`: `"YYYY-MM-DD HH:MI:SS"`
* `datetimeformattz`: `"YYYY-MM-DD HH:MI:SS"`

## Additional metadata files

Zero or more additional metadata files MAY be written to `[path]/_[any
name]`.  Additional metadata files MUST start with a `_` prefix

Why write metadata with `_` prefix?

* Spark and Hadoop treat `_` and `.` as hidden files when loading
  data from a path and will not treat them as data files.
* In particular, this means that we can write `records` outputs to
  adjacent subdirectories and tell Spark to load everything in
  there. As an example, we could write a folder each day:
  `/2017/06/12/data1.csv` and `/2017/06/13/data1.csv` and we could
  tell Spark to load the whole thing at `/2017/06/`. (What about
  eventual consistency you ask? Well-- this assumes that these batch
  reads are far enough in the future, that S3 has settled down.)
