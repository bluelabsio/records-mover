# Configuring records mover

There are two key areas where records mover needs configuration:

1. Temporary locations
2. Cloud credentials for object stores
3. Database connection details

## Database connection details

There are three ways to configure database connection details--some
are applicalbe only when using records mover as a Python library:

1. Setting environment variables (Python only)
2. Passing in pre-configured SQLAlchemy Engine objects (Python only)
3. Configuring db-facts (Python and mvrec)

### Setting environment variables (Python only)

The `Session` object contains a method called
`get_default_db_engine()` which will return a database engine as
configured by a set of env variables.  Note that using this method
limits you to dealing with one database at a time, and often requires
that env variables exist in your OS environment; if these trade-offs
aren't acceptable please see the other options below.

The default environment variables match the semantics of
[sqlalchemy.engine.url.URL](https://docs.sqlalchemy.org/en/13/core/engines.html#sqlalchemy.engine.url.URL)
and are as follows:

* `DB_HOST`
* `DB_DATABASE`
* `DB_PORT`
* `DB_USERNAME`
* `DB_PASSWORD`
* `DB_TYPE`

Redshift adds the following optional env variable(s):

* `REDSHIFT_SPECTRUM_BASE_URL_`: (optional) Specifies an `s3://` URL
  where Redshift spectrum files should be stored when using the
  'spectrum' records target.

BigQuery has an alternate set of env variables that should be used
instead of the `DB_` values above:

* `BQ_DEFAULT_PROJECT_ID`: Google Cloud Storage Project to be accessed.
* `BQ_DEFAULT_DATASET_ID`: BigQuery Dataset which should be used if
  not otherwise overridden.
* `BQ_SERVICE_ACCOUNT_JSON`: (optional): JSON (not a filename)
  representing BigQuery service account credentials.

### Passing in pre-configured SQLAlchemy Engine objects

The `database` factory methods for records sources and targets allow a
SQLALchemy Engine to be passed in directly.

### Configuring db-facts

[db-facts](https://github.com/bluelabsio/db-facts) is a complementary
project used to configure database credentials.  Please see
[db-facts documentation](https://github.com/bluelabsio/db-facts/blob/master/CONFIGURATION.md)
for details on configuration.

## Temporary locations

TODO: Finish the rest here

TODO: Add `SCRATCH_S3_URL` env variable

Cloud-based databases are often more efficient exporting to
cloud-native object stores (e.g., S3).

* `/etc/bluelabs/records_mover/app.ini`
* `/etc/xdg/bluelabs/records_mover/app.ini`
* `$HOME/.config/bluelabs/records_mover/app.ini`
* `./.bluelabs/records_mover/app.ini`
    #
    # There's also ability to configure this via env variables per the
    # XDG spec and config-resolver features:
    #
    # https://config-resolver.readthedocs.io/en/latest/intro.html#environment-variables
    #
    # Example file:
    #
    # [aws]
    # s3_scratch_url = s3://mybucket/


## Cloud credentials for object stores
