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
2. Replicate the current `postgres_itest` in `.circleci/config.yml`,
   including matching all of ther references to it.
  * Be sure to change the `with-db dockerized-postgres` line to refer
    to your database type.
  * Push up changes and verify that tests fail because your new
    database "is not a valid DB name".
  * Modify the `integration_test_with_dbs` job to include a Docker
    image for your new database.  If your database can't be run in a
    Docker image, you'll need some way to get to a database that can
    be used during integration testing.
  * Modify `tests/integration/circleci-dbfacts.yml` to point to your
    new integration test database account, whether in Docker or
    cloud-hosted.


3. Set up the `itest` script to be able to test as well.
  * Verify you get the same failures as from CircleCI.
