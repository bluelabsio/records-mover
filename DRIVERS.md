# Database drivers

Adding a database driver to records_mover can be divided up into three steps:

* Add integration testing and subclass DBDriver to get tests to pass
* Add code for native bulk import support
* Add code for native bulk export support

Here are the basic things you'll need to do to get through the
process.  Every database is different, and this document only gets
updated periodically, so you may run into additional things you need
to do or figure out.  If they seem like things people will hit in the
future, add them to this document!

## Basic support

1. Create a feature branch
2. Get a test database set up
  * Modify `tests/integration/docker-compose.yml` to include a Docker
    image for your new database.  If your database can't be run in a
    Docker image, you'll need some way to get to a database that can
    be used during integration testing.  Be sure to add a link from
    the 'records_mover' container to your new container.
  * Run `./itest-dc up -d` to bring up the docker-compose environment.
  * Run `./itest-dc start` to start the docker-compose environment.
  * Watch logs with `./itest-dc logs -f`
  * Fix any issues and repeat until it is successful and the logs look right.
2. Set up the `itest` script to be able to test your new database.
  * Modify the `local_dockerized_dbfacts` function in `./itest` to
    point to the new database.
  * Create a `wait-for-${your-new-db-type:?}.sh` script matching
    `wait-for-postgres.sh`.
  * Modify `tests/integration/inside-docker-dbfacts.yml` to include an
    entry for your new database.
  * Modify `tests/integration/bin/db-connect` to handle your new
    database type if needed.
  * Modify `Dockerfile` to add any new client binaries needed for your
    database and run `./itest --docker build` to build the new image.
  * Run `./itest shell`, which will start the docker-compose and start
    a shell with the db-facts you just created set.
  * Run `db ${your-new-db-name:?}` within that shell and verify it
    connects.
  * Exit out of the `./itest shell` session.
  * Run `./itest ${your-new-db-type:?}` and verify it doesn't
    recognize the argument.
  * Search down for instances of 'postgres' in the `itest` script and
    come up with the equivalent for your new database.
  * Run `./itest ${your-new-db-type:?}` again and verify thigns fail
    somewhere else (e.g., a Python package not being installed or a
    test failing most likely)
  * Push up your changes to the feature branch.
2. Now work to get the same failure out of CircleCI:
  * Replicate the current `postgres_itest` in `.circleci/config.yml`,
    including matching all of the references to it.
  * Be sure to change the `with-db dockerized-postgres` line to refer
    to your database type.
  * Push up changes and verify that tests fail because your new
    database "is not a valid DB name".
  * Note that you can (temporarily!) allow your new integration test
    to run without waiting for unit and Redshift tests to run by
    commenting out the dependency like this - just be sure to leave an
    annotation comment reminding you to fix it before the PR is
    merged!
    ```yaml
    #          requires:  # T ODO restore this
    #            - redshift-itest
    ```
  * Modify the `integration_test_with_dbs` job to include a Docker
    image for your new database, similar to `docker-compose.yml`
    above.
  * Modify `tests/integration/circleci-dbfacts.yml` to point to your
    new integration test database account, whether in Docker or
    cloud-hosted.
  * Iterate on the errors until you get the same errors you got in
    your `./itest` runs.
3. Fix these "singledb" tests!  Now that you have tests running (and
   failing), you can address the problems one by one.  Here are things
   you are likely to need to do--I'd suggest waiting for the problem
   to come up via the test and then applying the fix until the tests
   pass.  If you encounter things not on the list below, add them here
   for the next person (unless the fix you put in will address for all
   future databses with the same issue).
  * Add Python driver (either SQLAlchemy or if SQLAlchemy supports it
    natively, maybe just the DBAPI driver) as a transtive dependency
    in `setup.py`.  Rerun `./deps.sh` and then `./itest --docker
    build` to re-install locally.
  * If database connections aren't working, you may want to insert
    some debugging into `records_mover/db/connect.py` to figure out
    what's going on.
  * Access errors trying to drop a table in the `public` schema:
    Probably means whatever default schema comes with your database
    user doesn't match the default assumption - modify
    `tests/integration/records/single_db/base_records_test.py` to
    match.
  * `NotImplementedError: Please teach me how to integration test
    mysql`: Add information for your new database in
    `tests/integration/records/expected_column_types.py`,
    `tests/integration/records/mover_test_case.py`,
    `tests/integration/records/records_database_fixture.py` and
    `tests/integration/records/records_numeric_database_fixture.py`.
    This is where you'll start to get familiar with the different
    column types available for your database.  Be sure to be as
    thorough as practical for your database so we can support both
    exporting a wide variety of column types and so that we can
    support space-efficient use on import.

    For the numeric tests, when re-running you'll probably need to
    start filling out a subclass of DBDriver.  Relevant methods:
    `type_for_fixed_point()`, `type_for_floating_point()`,
    `fp_constraints()`, and `integer_limits()`.
  * `KeyError: 'mysql'` in
    `tests/integration/records/single_db/test_records_numeric.py`:
    There are test expectations to set here based on the numeric types
    supported by your database.  Once you set them, you'll probably
    need to add ad `type_for_integer()` method covering things
    correctly.
  * `AssertionError: ['INTEGER(11)', 'VARCHAR(3)', 'VARCHAR(3)',
    'VARCHAR(1)', 'VARCHAR(1)', 'VARCHAR(3)', 'VARCHAR(111)', 'DATE',
    'TIME', 'DATETIME', 'DATETIME']`: Double check the types assigned.
    You may need to subclass DBDriver and implement to convince
    records mover to create the types you expect.
  * Errors from `tests/integration/records/directory_validator.py`:
    ```console
    AssertionError:
    received ['integer', 'string', 'string', 'string', 'string', 'string', 'string', 'date', 'time', 'datetime', 'datetime'],
    expected [['integer', 'string', 'string', 'string', 'string', 'string', 'string', 'date', 'time', 'datetime', 'datetimetz'], ['integer', 'string', 'string', 'string', 'string', 'string', 'string', 'date', 'string', 'datetime', 'datetimetz']]
    ```

    To address, make sure the types returned are as expected for this database.
  * `KeyError: 'mysql'`:
    `tests/integration/records/single_db/test_records_numeric.py`
    needs to be modified to set expectations for this database type.
    You can set this to 'bluelabs' as we haven't yet taught
    records-mover to do bulk imports, so we have no idea what the
    ideal records format variant is for that yet.
  * `AssertionError` in
    `tests/integration/records/table_validator.py`: There are various
    checks here, including things dealing with how datetimes get
    rendered.  Examine carefully the existing predicates defined
    within and add new ones judiciously if it appears the behavior
    you are seeing is correct but not currently anticipated.
4. If there are things you see below that you know are needed from the
   above list, but the tests are passing, consider adding an
   integration test to match.
5. Edit
   `tests/integration/records/multi_db/test_records_table2table.py` to
   include the new test database and run `./itest table2table` to run
   tests.  Fix errors as they pop up.
7. Add support for bulk import if the database supports it (and add
   more detail here on how to do that!).
  * `tests/integration/records/single_db/test_records_numeric.py`
    needs to be modified to set the best loading records type for
    this database type - pick a type which can be loaded natively
    without using Pandas.
8. Add support for bulk export if the database supports it (and add
   more detail here on how to do that!).
