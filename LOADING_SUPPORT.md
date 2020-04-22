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

## Create an inital loading subclass

1. Modify your DBDriver subclass in `records_mover.db.your_db_type` to
   implement `load()`, `can_load_this_format()` and
   `known_supported_records_formats_for_load()` by deferring the calls
   to a new class we'll create in a bit.  You can crib this from an existing Make sure to include mypy
   types on all the calls.
2. Using the mypy errors as a driver (run `make` to get mypy to run),
   create these methods in the DBDriver subclass - initially with
   `raise NotImplementedError` as the body.  Focus for now at getting
   the right methods in place and wired up.
