# Setting up bulk loading support for a database type

## Read up

First, figure out whether and how the database supports exporting
data.  For instance, here's a documentation found by doing a web
search for this on MySQL:

* [LOAD DATA Statement](https://dev.mysql.com/doc/refman/8.0/en/load-data.html)

Some key things to look for as you start in:

* What "records formats" are supported?  Delimited ("CSV")?  Parquet?
  Others?  Adding support for as many as feasible in records mover
  will increase the chances that records mover can move data without
  having to bring it into memory first.
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

## Create some scaffolding

1. Create an empty subclass of either
   `records_mover.db.loader.LoaderFromRecordsDirectory` or
   `records_mover.db.loader.LoaderFromFileobj` in
   `records_mover.db.your_db_type`, similar to other database types.  You can ignore
   the MyPy errors for now.
2. Modify `loader()` in your `DBDriver` subclass to return an instance
   of your class.  If you are implementing `LoaderFromFileobj`, also
   have `loader_from_fileobj()` return it.
3. Using the mypy errors as a driver (run `make` to get mypy to run),
   create these methods in the DBDriver subclass - initially with
   `raise NotImplementedError` as the body.  Focus for now at getting
   the right methods in place, not yet on adding actual logic inside
   them.

## Get tests wired up and talking to your code

1. Run the `./itest` target for your database.  You should expect to
   see a lot of errors from your NotImplementedErrors.  Great!  That
   means you wired things up correctly to use your loader, which is
   all we're testing at this point.
2. Assuming your database uses SQL or maybe a driver function call to
   initiate bulk loads, this is your chance to figure out how how
   you're going to generate the different SQL or function call
   arguments based on what different RecordsFormat options out there.

   Create a function signature describing that approach, in a new file
   (e.g., `records_mover.db.your_db_type.load_options`) in your Loader
   class.  Name it (and the file it's in) after the SQL or function call
   syntax specific to your database - e.g., the Redshift function is named
   after Redshift's `UNLOAD` statement.

   This function should just raise `NotImplementedError` for now.
   Example:

   ```python
   def redshift_unload_options(unhandled_hints: Set[str],
                               records_format: BaseRecordsFormat,
                               fail_if_cant_handle_hint: bool) -> RedshiftUnloadOptions:
   ```

   In the case above, there was an existing function to do unloading,
   and `RedshiftUnloadOptions` is a MyPy `TypedDict` describing the
   keyword arguments to that function.

   If you're constructing SQL yourself, you might consider making a
   class to do it, perhaps subclassed from `typing.NamedTuple`.  See
   `db.mysql.load_options.MySqlLoadOptions` for an example of that.

   If this handles any RecordsFormats which include hints (e.g., for
   `DelimitedRecordsFormat`), have this method also accept arguments
   of `unhandled_hints: Set[str]` and `fail_if_cant_handle_hint:
   bool`.

   You should make the `records_format` argument a `Union` of whatever
   specific formats your database supports.

2. Go ahead and either write the top-level `load()` or
   `load_from_fileobj()` function in your loader - depending on
   whether your database driver takes a filename/URL or a stream,
   respectively.

   If your driver takes a filename/URL, follow the redshift driver
   example on how to translate the URLs passed in `load()` to the
   right scheme.

   Delegate out the actual generation of the tricky parts of the
   e.g. SQL statement to another method which takes as input e.g. the
   `RedshiftUnloadOptions` output of the function you wrote before.
   This new function should just `raise NotImplementedError` for now.

3. Now we're going to create a first unit test, modeled on
   `tests/unit/db/mysql/test_mysql_load_options_known#test_load_known_formats`.
   Verify that it fails due to the plethora of `NotImplementedError`s
   raised.

4. Start by resolving the `NotImplementedError` raised from your
   `known_supported_records_formats_for_load()` method.  For now we're
   going to assume the very best case - that your database will
   support every single records format we support--or at least the
   particularly useful ones.  This will no doubt not be true by the time
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

5. Run the test and start addressing issues one by one by implementing
   the function in question.  This will be slow, finicky work - do
   yourself a favor and leave yourself copious notes as comments
   pointing back to the original online documentation for the database
   in question as you go.  Follow the other drivers' use of
   `cant_handle_hint()` and `quiet_remove()`.  The MySQL loader in
   particular uses hint typing to ensure we are covering cases
   correctly, and has written types for the valid input into MySQL's
   LOAD statement as well.

   To get the test to pass, you may need to tweak the formats slightly
   (e.g., changing date formats to ones supported or specifying no
   compression).  You may not be able to support certain variants or
   formats at all - leave a comment in the method documenting all of
   these situations and describing why.

6. Write a unit test for `can_load_this_format()` - see
   `tests/unit/db/mysql/test_mysql_db_driver.py` for an example.  Now
   implement `can_load_this_format()` in a similar way to other
   drivers and verify the tests pass.

7. Figure out which functions are left raising NotImplementedError, if
   any.  Write a bunch of T ODO comments documenting the different
   cases that need to be handled.  Include cases for invalid
   combinations (expect them to raise exceptions).  Write a test for
   the first one.  Implement enough to get that test to pass, and then
   lather, rinse and repeat.

8. Get a sample load working with `mvrec`.  Having your
   already-working proof of concept to cross debug with will be vital
   at this point, unless you're one of those lucky engineers for whom
   everything works the first time...

9. Run `./itest` with your database target and work through any issues
   found.

10. Get unit tests passing by updating to match whatever changes you
    needed to make for the integration tests.

11. Increase unit test coverage to match or exceed the current high
    water mark.  You can look at `cover/index.html` and sort by
    'missing' to see likely reasons why coverage has slipped.

12. Update `mover_test_case.py#supported_load_variants` in
    `tests/integration` to reflect expectations for your database

13. Get CircleCI passing through the 'test' workflow.

14. Get CircleCI passing through the 'quality' workflow.  You can work
    through issues locally with `make quality` and friends.
