# Configuring records mover

There are key areas where records mover needs configuration:

1. Database connection details
2. Temporary locations
3. Cloud credentials (e.g., S3/GCS/Google Sheets)

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
URL or configure a INI-style file in one of the following locations:

* `/etc/bluelabs/records_mover/app.ini`
* `/etc/xdg/bluelabs/records_mover/app.ini`
* `$HOME/.config/bluelabs/records_mover/app.ini`
* `./.bluelabs/records_mover/app.ini`

Example file:

```ini
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

## Cloud credentials (e.g., S3/GCS/Google Sheets)

To be able to access cloud resources, including S3, GCS and Google
Sheets, Records Mover requires credentials.

There are multiple ways to configure these:

1. Vendor system configuration (Python and mvrec)
2. Setting environment variables (Python only)
3. Passing in pre-configured default credential objects (Python only)
4. Using a third-party secrets manager (Python and mvrec)
5. Airflow connections (Python via Airflow)

### Vendor system configuration (Python and mvrec)

Both AWS and GCP have Python libraries which support using credentials
you configure in different ways.  Unless told otherwise, Records Mover
will use these credentials as the "default credentials" available via
the 'creds' property under the Session object.

### Setting environment variables (Python and mvrec)

AWS natively supports setting credentials using the
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`
environment variables.

Similarly, GCP supports pointing to a file with application
credentials via the `GOOGLE_APPLICATION_CREDENTIALS` environment
variable.

When using the default 'env' session type, Records Mover also supports
providing a base64ed version of the GCP service account credentials via
the `GCP_SERVICE_ACCOUNT_JSON_BASE64` env variable.

### Passing in pre-configured default credential objects (Python only)

You can pass in credentials objects directly to a Session() object
using the `default_gcs_client`, `default_gcp_creds` and/or
`default_boto3_session` arguments.

### Using a third-party secrets manager (Python and mvrec)

To use a secrets manager of some type, you can instruct Records Mover
to use a different instance of the 'BaseCreds' class which knows how
to use your specific type of secrets manager.

An [example implementation](https://github.com/bluelabsio/records-mover/blob/master/records_mover/creds/creds_via_lastpass.py)
ships with Records Mover to use LastPass' CLI tool to fetch (for
instance) GCP credentials via LastPass.

You can either pass in a instance of a BaseCreds subclass as the
'creds' argument to the Session() constructor in Python, pass in the
string 'lpass' as the value of the 'session_type' parameter to the
Session() constructor, or provide the following config in the `.ini`
file referenced above:

```ini
[session]
session_type = "lpass"
```

### Airflow connections (Python via Airflow)

Similarly, Records Mover ships with a BaseCreds instance which knows
how to fetch credentials using Airflow connections.  While Records
Mover will attempt to auto-detect to determine if it is running under
Airflow, you can explicitly tell Records Mover to use this mode by
setting session_type to "airflow" using one of the above methods.
