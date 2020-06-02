# Configuring records mover

There are key areas where records mover needs configuration:

1. Database connection details
2. Temporary locations
3. Cloud credentials for object stores

## Database connection details

There are ways to configure database connection details--some are
applicable only when using records mover as a Python library:

1. Setting environment variables (Python only)
2. Passing in pre-configured SQLAlchemy Engine objects (Python only)
3. Configuring db-facts (Python and mvrec)
4. Airflow connections (Python via Airflow)

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

### Passing in pre-configured SQLAlchemy Engine objects (Python only)

The `database` factory methods for records sources and targets allow a
SQLALchemy Engine to be passed in directly.

### Configuring db-facts (Python and mvrec)

[db-facts](https://github.com/bluelabsio/db-facts) is a complementary
project used to configure database credentials.  Please see
[db-facts documentation](https://github.com/bluelabsio/db-facts/blob/master/CONFIGURATION.md)
for details on configuration.

### Airflow connections (Python via Airflow)

If you are running under Airflow, the
`session.creds.get_db_engine(name)` method will look up `name` in your
Airflow connections rather than use `db-facts`.  This can be
configured via the `session_type` parameter passed to the `Session()`
constructor.

## Temporary locations

Cloud-based databases are often more efficient exporting to
cloud-native object stores (e.g., S3) than otherwise.  Indeed, some
(e.g., Redshift) *only* support exporting to and importing from an
object store.  In order to support moves between such databases and
incompatible targets, records mover must first export to the
compatible object store in a temporary location.

Note that you'll need credentials with permission to write to this
object store - see below for how to configure that.

### S3 (Redshift)

To specify the temporary location for Redshift exports and imports,
you can either set the environment variable `SCRATCH_S3_URL` to your
URL or configure a TOML-style file in one of the following locations:

* `/etc/bluelabs/records_mover/app.ini`
* `/etc/xdg/bluelabs/records_mover/app.ini`
* `$HOME/.config/bluelabs/records_mover/app.ini`
* `./.bluelabs/records_mover/app.ini`

Example file:

```toml
[aws]
s3_scratch_url = "s3://mybucket/path/"
```

If you want to use a single scratch bucket for multiple individuals
without granting them the ability to see each others' files, you can
use this configuration:

```toml
[aws]
s3_scratch_url_appended_with_iam_username = "s3://mybucket/home/"
```

In this case, creds tied to an AWS user named `first.last` would end
up with a scratch bucket location of `s3://mybucket/home/first.last/`.
This will allow use of the permissioning pattern which AWS has
documented
[here](https://aws.amazon.com/blogs/security/writing-iam-policies-grant-access-to-user-specific-folders-in-an-amazon-s3-bucket/).

### Filesystem

Temporary files written to the filesystem (including large data files
downloaded for local processing) will be stored per Python's
[tempfile](https://docs.python.org/3/library/tempfile.html) default,
which allow for configuration via the `TMPDIR`, `TEMP` or `TMP` env
variables, and generally default to
[something reasonable per your OS](https://docs.python.org/3/library/tempfile.html#tempfile.gettempdir).
