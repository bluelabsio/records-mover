# Database drivers

Adding a database driver to records_mover can be divided up into three steps:

* Add integration testing and subclass DBDriver to get tests to pass
* Add code for native bulk import support
* Add code for native bulk export support

## Basic support

1. Create a feature branch
2. Replicate the current `postgres_itest` in `.circleci/config.yml`,
   including matching all of ther references to it.
  * Be sure to change the `with-db dockerized-postgres` line to refer
    to your database type.
  * Push up changes and verify that tests fail because the database
    type wasn't found.  We can fix that!
