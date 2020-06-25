# Test data format

To test different corner cases of handling data which typically
require escaping/quoting to represent in delimited format (and, to a
lesser degree, our chain of handling this sort of data via databases),
we have a standard one-row records format data item used throughout
the tests.  Not all data systems can represent everything here, but
this is what we would ideally represent for this test:

* num: This is an integer representing one hundred and twenty three.
* numstr: This is the string `123`
* str: This is the string `foo`
* comma: This is the string `,`
* doublequote This is the string `"`
* quotecommaquote: This is the string `","`
* newlinestr: The string as follows:

```* SQL unload would generate multiple files (one for each slice/part)\
* Filecat would produce a single data file
```

* date: This represents January 1st in the year 2000 AD.
* time: This represents a timezoneless, dateless time of midnight.
* timestamp: This represents the timezoneless moment of the second of
  January, in the year 2000 AD, at 789012 microseconds past 56 seconds
  past 34 minutes past noon.
* timestamptz: This represents the moment of the second of January, in
  the year 2000 AD, at 789012 microseconds past 56 seconds past 34
  minutes past noon, in the US/Eastern timezone (when lunch
  restaurants in DC are busy).

  On systems where a timezone can't be represented, this should be
  represented as if the implicit timezone was US/Eastern.

## Filename suffixes

Depending on details of the move and capabilities of the database,
there are some differences in output - mainly surrounding how
timezones in timestamps are expressed.

### pandas

* Expresses time zone as e.g., +0000 with no colons or shortening
  when zero-padded.
* Doesn't allow specification of the format of dates or
  times, just datetimes, so dates and times come out in
  default Pandas __str__() format.

  See details on [Pandas limitations](https://github.com/bluelabsio/records-mover/issues/83)

### utc

* the timestamp sent into the database with a timezone comes out in
  UTC time, generally because either the database didn't store the
  timezone (after converting the input to UTC) or because some step in
  output (e.g., older Vertica S3 output code?) didn't preserve the two
  separate bits of information.

  When combined the format variant like 'CSV' that doesn't include a
  timezone offset in timestamptz output, this will result in the time
  output being the UTC time without offset (so, "17:34:56.789012" for
  our fixtures).  Otherwise they would appear as "12:34:56.789012",
  the time in the US/Eastern timezone that our fixtures assign.

### postgres

Postgres (in the "text" mode we use for 'bluelabs' format, at least)
represents newlines as '\n' instead of '\' with an actual newline
afterwards like other databases do.

### notz

* Some databases (e.g., MySQL) don't have a generally usable column
  type for our `datetimetz` type.  As a result, when data is exported
  from a table without a records schema defined, it's of course not
  going to have a timezone offset recorded.
