# Setting up bulk loading support for a database type

## Read up

First, figure out whether and how the database supports exporting
data.  For instance, here's a documentation found by doing a web
search for this on MySQL:

* [LOAD DATA Statement](https://dev.mysql.com/doc/refman/8.0/en/load-data.html)

Some key things to look for as you start in:

* What "records formats" are supported?  Delimited?  Parquet?  Others?
  Adding support for as many as feasible in records mover will
  increase the chances that records mover can move data without having
  to bring it into memory first.
* Some databases only support loading from files already on the
  database server.  Records mover does not support this mode, as not
  everyone has access to the database server itself.
* Some databases support syntax pointing to a specific file on disk.
  Some support loading from a stream.  The latter is more efficient,
  as records mover can potentially start loading data immediately
  instead of waiting to download it all first.
* Bulk loading requires some DBAPI support specifically for this mode.
  Sometimes there's even a SqlAlchemy Command class that has been
  cooked up to support.  Read the docs on your DBAPI driver and look
  to see if there's such a Command class.

## Build a proof of concept using Python drivers

Create a small Python file that tries to load a trivial file into a
table to show you can get a file off of your local machine and into
the database.

Along the way, figure out which one of of these your database wants most to do:

* Load from a path on disk or URL (e.g., S3)
* Load from a file stream (a "fileobj" in Python)

[done to here]

## If via a path on disk or URL

# TODO: Create an ABC for each of the two chunks below and point people on that to implement?

Modify your DBDriver subclass in `records_mover.db.your_db_type` to
implement `load()`, `can_load_this_format()`,
`can_load_from_fileobjs()`, `best_scheme_to_load_from()` and
`temporary_loadable_directory_loc()`.

## If via a file stream

Modify your DBDriver subclass in `records_mover.db.your_db_type` to
implement ``load_from_fileobj()` and by deferring the calls to a new
class we'll create in a bit.  You can crib this from an existing
driver.

## Continue building out

1. Modify your DBDriver subclass in `records_mover.db.your_db_type` to
   implement `known_supported_records_formats_for_load()` by deferring
   the calls to a new class we'll create in a bit.  You can crib this
   from an existing driver.

## Create an inital loading subclass

2. Using the mypy errors as a driver (run `make` to get mypy to run),
   create these methods in the DBDriver subclass - initially with
   `raise NotImplementedError` as the body.  Focus for now at getting
   the right methods in place and wired up.

## Get tests wired up and talking to your code

1. Run the `./itest` target for your database.  You should expect to
   see a lot of errors from your NotImplementedErrors.  Great!  That
   means you wired things up correctly to use your loader.

[done to here]

2. Go ahead and either write the top-level `load()` or
   `load_from_fileobj()` function in your loader - depending on
   whether your database driver takes a stream or a filename/URL.  If
   your driver takes a filename/URL, follow the redshift driver
   example on how to translate the URLs passed in `load()` to
   the right scheme.

   Delegate out the actual generation of the tricky parts of the SQL
   statement to another method, which should `raise
   NotImplementedError` for now.
3. Now we're going to create a first unit test, modeled on
   `unit/db/postgres/test_postgres_copy_options_load_known.py`.
   Verify that it fails due to the plethora of `NotImplementedError`s
   raised.
4. Start by resolving the NotImplementedError raised from your
   `known_supported_records_formats_for_load()` method.  For now we're
   going to assume the very best case - that your database will
   support every single records format we support--or at least the
   particularly good ones.  This will no doubt not be true by the time
   we get done implementing - but the tests will catch that and we'll
   tweak this down to what's actually supported by the time we're
   done.  No need to implement anything else yet.

   ```python
   return [
       ParquetRecordsFormat(),
       DelimitedRecordsFormat(variant='bluelabs'),
       DelimitedRecordsFormat(variant='csv'),
       DelimitedRecordsFormat(variant='bigquery'),
       DelimitedRecordsFormat(variant='vertica'),
   ]
   ```

## Start






   Take your best crack at
   things and leave TOD O comments as you go for uncovered
   situations - we'll have integration tests to help drive this to
   completion.
