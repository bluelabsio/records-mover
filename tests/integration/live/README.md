# Live acceptance suite

Opt-in tests that move real data through real Redshift and S3 with
records-mover and verify row counts, values and column types.  This is what
"works" means for the project (see `docs/ROADMAP.md`).

Nothing runs unless `RECORDS_MOVER_LIVE=1`.  Without it every live test is
skipped (only the pure `test_guard_selftest.py` tests run).  These tests are
not part of `make unit` / `make component`.

## Running

```sh
export RECORDS_MOVER_LIVE=1
export RECORDS_MOVER_LIVE_DBT_PROFILE=my_profile
export RECORDS_MOVER_LIVE_DBT_TARGET=dev
export RECORDS_MOVER_LIVE_SCHEMA=my_scratch_schema
export RECORDS_MOVER_LIVE_ALLOWED_SCHEMAS=my_scratch_schema,dbt_my_scratch_schema
export RECORDS_MOVER_LIVE_S3_PREFIX=s3://my-scratch-bucket/records-mover-test/   # optional
make live        # or: ENV=test pytest tests/integration/live -rs
```

Expect it to be slow: INSERTs through a SQL proxy cost about 130 ms per row and each
column reflection about 7 s.  Data is kept to 1000 rows or fewer.

## Environment variables

| Variable | Required | Meaning |
|---|---|---|
| `RECORDS_MOVER_LIVE` | yes | Must be exactly `1` to enable the suite. |
| `RECORDS_MOVER_LIVE_DB_URL` | one of these two | SQLAlchemy URL, e.g. `redshift+psycopg2://user:pass@host:5439/db`. |
| `RECORDS_MOVER_LIVE_DBT_PROFILE` + `RECORDS_MOVER_LIVE_DBT_TARGET` | | Read the connection from a dbt profile (`~/.dbt/profiles.yml`, or `RECORDS_MOVER_LIVE_DBT_PROFILES`). dbt `{{ env_var('X') }}` values are resolved. |
| `RECORDS_MOVER_LIVE_SCHEMA` | yes | Schema the tests create tables in. |
| `RECORDS_MOVER_LIVE_ALLOWED_SCHEMAS` | yes | Comma-separated. `RECORDS_MOVER_LIVE_SCHEMA` must be listed; writes elsewhere are blocked. |
| `RECORDS_MOVER_LIVE_S3_PREFIX` | no | e.g. `s3://bucket/records-mover-test/`. S3 tests are skipped when unset. |
| `RECORDS_MOVER_LIVE_AWS_PROFILE` | no | AWS profile used to mint temporary credentials (default `default`). |
| `RECORDS_MOVER_LIVE_SSLMODE` | no | Default `verify-full`; `disable` turns TLS settings off. |
| `RECORDS_MOVER_LIVE_SSLROOTCERT` | no | Root cert file; default is `certifi.where()`. |
| `RECORDS_MOVER_LIVE_STATEMENT_TIMEOUT_S` | no | `SET statement_timeout` per connection (default 120). |

No secrets live in the repo.  Long-lived AWS keys are only used to call
`sts.get_session_token(DurationSeconds=900)`; records-mover only ever sees
the resulting temporary credentials (minted fresh for each S3 test).

## Safety

`guard.py` attaches a SQLAlchemy `before_cursor_execute` listener to every
engine, checking the statement **and bound parameters** (sqlalchemy-redshift
passes UNLOAD's inner select and S3 URL as bound parameters).  It raises,
before anything reaches Redshift, if:

* an `s3://` URL is outside the run's prefix (non-S3 tests allow none);
* a write (CREATE/DROP/INSERT/DELETE/TRUNCATE/ALTER/GRANT/REVOKE/UPDATE/COPY/
  UNLOAD/...) does not reference `<schema>.rm_test_*`, or references any other
  schema-qualified table;
* `IAM_ROLE` appears (not expected yet).

Table names are `rm_test_<8 hex>_<label>` and S3 objects live under
`<prefix><8 hex>/`, so parallel runs do not collide.  The suffix identifies
leftovers if a run is killed.

Secrets (the minted AWS keys and token) are registered for redaction; a
log-record factory and a pytest report hook scrub them, since SQLAlchemy
errors embed the SQL.

## Cleanup

Always runs (session fixture finalizer): drops every `rm_test_` table the run
created and deletes every S3 object under the run's sub-prefix.  Failures fail
the session loudly.  Per-test engines force-close all their connections
first, because records-mover's drivers hold connections with AccessShareLocks
that would block `DROP TABLE`.  Each connection sets `statement_timeout`, so a
lock wait cannot hang forever.

## Known gaps

`test_dataframe_object_columns_keep_their_types` is `xfail(strict=True)`:
DataFrame `Decimal`/`date` object columns currently land as `varchar`.  It
will fail the suite (XPASS) when the bug is fixed - remove the marker then.
