# Revival Assessment

This assessment was done on 2026-09-29 against `main` at `43f79f8`. The last commit on `main` was 2024-06-18, and the last release was v1.6.4 on 2024-05-14.

## Intended scope

The revived records-mover is an **internal-only** 14th tool. Its main job is to help Data Science (DS) get data into and out of Redshift. It is not a client delivery tool. Company leadership confirmed this on 2026-09-29.

This scope sets the priorities for the work below:

- **Core path (must work):** Redshift load and unload (`db/redshift`, `COPY`/`UNLOAD` via S3), S3 URLs, delimited/CSV files and records directories, and pandas DataFrames, since DS works in Python.
- **Secondary:** PostgreSQL (it is also the local stand-in for Redshift in tests), Parquet, and the CLI.
- **Candidates to deprioritize or drop:** BigQuery/GCS, Vertica, MySQL, Google Sheets, Redshift Spectrum, and the Airflow hooks and credentials. Dropping these would also remove a large share of the dependency load: the Airflow providers, the Google Cloud client libraries, `sqlalchemy-vertica-python`, and `libcst` (the package behind breakage 1). The team should decide which of these to drop before modernizing.

## Summary

On Python 3.9 the core library still works, but it no longer installs cleanly, and its test and typecheck steps fail as configured. It does not install on any currently supported Python version.

A handful of small fixes (see [Quick fixes](#quick-fixes-on-python-39)) would get it working again on 3.9. A real revival means moving to Python 3.11+. That forces pandas 2 and numpy 2, and it makes sense to do the SQLAlchemy 2 migration in the same pass.

The repo's integration suite was **not** run. Instead, a guarded live smoke test ran against a 14th development Redshift cluster over the non-S3 (INSERT/SELECT) path, and all 7 scenarios passed (see [Redshift](#redshift-live-non-s3-path)). A second run tested the S3 `COPY`/`UNLOAD` path, which DS would rely on for large data, and all of its load and unload scenarios passed. That run also found performance and locking problems (see [S3 path](#redshift-live-s3-copyunload-path)).

## How it was tested

- Scratch virtualenvs were built on Python 3.9 and 3.12 with `pip install -e '.[unittest,typecheck]' -r requirements.txt`.
- Tests: `pytest tests/unit`, `pytest tests/component`.
- Typecheck and lint: `mypy records_mover`, `flake8`.
- Manual CLI moves with `mvrec`: `file2file` and `file2recordsdir` on small CSVs.

## What works (Python 3.9)

- Unit tests: 514 of 515 pass. Component tests: 243 of 243 pass. Both need the warnings workaround below.
- The library imports, and the `mvrec` CLI builds all 30 source→target subcommands.
- CSV → CSV (`file2file`) works.
- CSV → records directory (`file2recordsdir`) works for files whose header row is detected, and it writes `_manifest`, `_format_delimited`, and `_schema.json`.
- `flake8` passes.

### Redshift (live, non-S3 path)

On 2026-09-29, a guarded smoke test ran against a 14th development Redshift cluster, reached through a SQL access proxy. It used the current code on Python 3.9 (pandas 1.5.3, SQLAlchemy 1.4.54).

The test was scoped to temporary `rm_test_*` tables in a personal scratch schema, and every table was dropped afterward. AWS access was disabled, so only the INSERT/SELECT path was tested. **S3 `COPY`/`UNLOAD`, the fast path, has not been tested yet.**

A SQLAlchemy `before_cursor_execute` hook checked every statement before it ran. It would have rejected any S3 reference and any write outside `rm_test_*` in the allowed schemas. It blocked nothing during the run.

**All 7 scenarios passed:**

| # | Scenario | Result |
|---|---|---|
| 1 | DataFrame → new table (CREATE TABLE is generated automatically) | Pass. See the type fidelity note below |
| 2 | Table → DataFrame | Pass |
| 3 | Table → table (same schema) | Pass. The source and target contents are identical |
| 4 | CSV file → new table (format and types inferred) | Pass. Quoted fields containing commas were handled, and `id`/`population` were created as `bigint` |
| 5 | Existing table, `DELETE_AND_OVERWRITE` | Pass |
| 6 | Existing table, `APPEND` | Pass |
| 7 | Existing table, `DROP_AND_RECREATE` | Pass |

### Redshift (live, S3 `COPY`/`UNLOAD` path)

A second guarded run on 2026-09-29 exercised the S3 path, using a test prefix in a personal scratch bucket. Because records-mover puts AWS credentials into the `COPY`/`UNLOAD` SQL text, which may be logged by the proxy and in Redshift's query history, it ran on 15-minute `GetSessionToken` credentials rather than the long-lived IAM user keys. All test tables and S3 objects were removed afterward.

| # | Scenario | Path records-mover chose | Result |
|---|---|---|---|
| 1 | DataFrame → new table (1,000 rows, including quoted values that contain commas and NULLs) | CSV → S3 temp dir → `COPY` | Pass |
| 2 | Table → S3 records directory → new table | `UNLOAD` (with manifest and `_schema.json`) → `COPY` directly from that directory | Pass. The round trip preserves the data exactly |
| 3 | CSV file → new table | Rewrite to CSV → S3 temp dir → `COPY` | Pass |
| 4 | Table → table | `UNLOAD` to a temp location → `COPY` | Pass |
| 5 | Table → DataFrame | `SELECT` (reading into DataFrames never uses S3) | Slow. See below |

**Problems found during these runs:**

- **The `INSERT` fallback is unusably slow through the proxy.** 1,000 rows took about 130 seconds, because pandas `to_sql` issues one network round trip per row. Anything beyond small data must use the S3 path, so a scratch bucket plus AWS credentials should be the supported configuration for DS.
- **Column metadata lookups cost about 7 seconds each.** sqlalchemy-redshift's `dialect.get_columns` takes about 7 seconds per call on this cluster, against 0.26 seconds for a plain `SELECT`. records-mover makes several such calls per operation, so reading a 1,000-row table into DataFrames took about 34 seconds, almost all of it spent on these lookups.
- **Connections are left holding locks.** The drivers keep long-lived connections (`driver.db_conn`). After a `SELECT` in SQLAlchemy 1.4's legacy autocommit mode, SQLAlchemy reports no open transaction, but psycopg2 has opened one, and it holds an `AccessShareLock` until the connection closes. A later `DROP`, `TRUNCATE`, or `ALTER` on the same table from another connection blocks. For example, `DROP_AND_RECREATE` after a read can hang. Transaction handling needs rework in the SQLAlchemy 2 migration anyway.
- **Credentials are embedded in SQL.** `COPY`/`UNLOAD` pass `ACCESS_KEY_ID`/`SECRET_ACCESS_KEY`/`SESSION_TOKEN` inline (`db/redshift/loader.py:95`, `unloader.py:90`). This can put keys into query history and audit logs. The revival should add support for `IAM_ROLE`, which lets Redshift assume a role itself, so no keys ever appear in SQL.

**Type fidelity from DataFrames:** columns with the pandas `object` dtype are created as `varchar`. That covers Python `Decimal` and `datetime.date` values, so `Decimal('1.50')` is stored as `'1.5'` and dates become strings. The `int`, `float`, `bool`, and `datetime64` columns map correctly. DS users loading DataFrames will hit this, so it should be fixed or documented during the revival.

**Connection notes:**

- **TLS:** the SQL proxy presents a certificate from a public CA. The `psycopg2-binary` wheel's libpq fails verification under `sslmode=require`. It works with `sslmode=verify-full` and `sslrootcert=certifi.where()`.
- **Identity:** Redshift sees the proxy's service account, not the individual user, and that account has its own default schema. records-mover qualifies every table with its schema, so writes do not land in that default schema. Callers should still always pass `schema_name` explicitly.

## What's broken

1. **The 3.9 install fails from source.** `libcst`, pulled in through Airflow, has no prebuilt package for 3.9, so pip compiles it with Rust and fails. The workaround is `pip install --only-binary libcst,pandas,numpy,pyarrow,psycopg2-binary ...`.
2. **None of the tests load as configured.** All 162 test files fail during pytest collection. The code imports `mypy_extensions.TypedDict` in 7 files, and that now raises a `DeprecationWarning`. `pytest.ini` turns warnings raised from `records_mover` into errors. Because Python attributes this warning to `records_mover`'s own code, `-W` filters don't suppress it. The workaround is `-o filterwarnings=ignore::DeprecationWarning`. The fix is to import `TypedDict` from `typing_extensions`.
3. **Airflow 3 changes session detection.** `apache-airflow>=2` has no upper bound, so pip now installs Airflow 3.x. Importing `records_mover.airflow.hooks` under Airflow 3 sets `AIRFLOW__CORE__EXECUTOR`, and `session.py:_infer_session_type()` then decides the session type is `airflow` instead of `env`. As a result, `tests/unit/test_session_choices.py::test_select_cli_session_by_default` fails in the full suite but passes alone. This is also a real behavior bug for anyone who has Airflow 3 installed.
4. **`make typecheck` fails with 22 errors.**
   - 18 are `Enum members must be left unannotated`, raised by newer mypy against the local stubs in `types/stubs/inspect.pyi` and `types/stubs/sqlalchemy_redshift/commands.pyi`.
   - 3 are unneeded `type: ignore` comments.
   - 1 is in real code: a conflicting `containing_directory` definition in `records_mover/url/s3/s3_directory_url.py:8`.
5. **Writing the JSON schema crashes when no header row is detected.** For example, a 2-column CSV `a,b / 1,x / 2,y` fails. The columns get `numpy.int64` names, and `json.dumps` in `records/schema/schema/__init__.py:88` raises `TypeError: keys must be str...`. Passing `--source.header_row` avoids it.
6. **CSV → Parquet writes an empty file with no error.** Exporting from a DataFrame to Parquet is not implemented (`NotImplementedError: Teach me to export from dataframe to parquet`). This is not a regression, but a 0-byte output file with no failure is misleading.
7. **Python 3.12 won't install.** The `pandas<2` pin has no prebuilt packages for 3.12, and building pandas 1.5 from source fails (`No module named 'pkg_resources'`). Separately, `setup.py` imports `distutils`, which was removed in Python 3.12.

## How out of date the dependencies are

| Package | Repo requires | Installed on 3.9 | Latest (2026-09) | Notes |
|---|---|---|---|---|
| Python | 3.8–3.9 | 3.9 | 3.14 | 3.8 and 3.9 are end of life. Most current releases below require 3.10 or newer |
| pandas | `>=1.3.5,<2` | 1.5.3 | 3.0.6 (needs Python 3.11+) | `line_terminator` (removed in 2.0) is used in `records/pandas/to_csv_options.py:119` and `read_csv_options.py:507` |
| numpy | `<2` (RM-133) | 1.26.4 | 2.5.3 (needs Python 3.12+) | Upgrade together with pandas |
| SQLAlchemy | `>=1.4,<2` | 1.4.54 | 2.1.1 | Largest migration. The DB drivers are written against the 1.x API. `sqlalchemy-stubs` is dead, and SQLAlchemy 2 ships its own types |
| apache-airflow | `>=2` | 3.0.6 | 3.3.2 | Airflow 3 is already installed and causing breakage 3. It needs a cap, or code changes to support 3 |
| sqlalchemy-redshift | `>=0.7.7` | 0.8.14 | 1.0.0 | Major version bump |
| sqlalchemy-bigquery | any | 1.16.0 | 1.17.2 | |
| pyarrow | any | 21.0.0 | 25.0.1 | |
| smart_open | `>=2` | 7.5.0 | 8.0.2 | |
| mypy | `>=1.7.1` | 1.19.1 | 2.3.1 | |
| pytest | `<8.2` | 8.1.2 | 9.1.1 | The cap blocks newer releases |

These dependencies are unmaintained or abandoned:

- `config-resolver` (last release 2021; the repo caps it `<6`)
- `timeout_decorator` (2020)
- `odictliteral` (2014; plain `dict` is ordered, so it can be replaced)
- the `google` package (2020)
- `sqlalchemy-vertica-python` (2023)
- `db-facts` (2024, a BlueLabs/14th package)

CI and tooling are also out of date:

- CI is split between GitHub Actions (unit tests and docs, Python 3.8/3.9) and CircleCI (integration tests and publishing).
- CircleCI uses `mysql:5` and `jbfavre/vertica:8.1.1` images.
- The quality gem image is `python-37`.
- `pyproject.toml` still lists Python 3.6/3.7 classifiers.

## Recommended path

### Quick fixes on Python 3.9

These get it working again without upgrading anything major:

1. Replace `mypy_extensions.TypedDict` with `typing_extensions.TypedDict`.
2. Cap `apache-airflow<3`, or fix the environment-variable side effect and support Airflow 3.
3. Fix the 22 mypy errors, most of which are mechanical stub cleanups.
4. Cast column names to `str` before writing the JSON schema.
5. Fail loudly on the Parquet export path instead of writing an empty file.

### Modernizing

1. Decide which backends outside the core Redshift path to drop (see [Intended scope](#intended-scope)). This shrinks the dependency and test surface before any upgrade work starts.
2. Target Python 3.11+ and drop 3.8/3.9. Replace the `distutils` ratchet commands in `setup.py`.
3. Upgrade to pandas 2 and numpy 2: rename `line_terminator` to `lineterminator`, then audit the dtype and `read_csv` date-parsing options.
4. Migrate to SQLAlchemy 2: move to 2.0-style execution, drop `sqlalchemy-stubs`, and update the redshift, bigquery, and vertica dialects.
5. Replace or vendor the abandoned dependencies.
6. Consolidate CI onto GitHub Actions and refresh the database images.
