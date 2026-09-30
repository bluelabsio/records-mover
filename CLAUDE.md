# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Records Mover is a Python library and CLI (`mvrec`) for moving "rectangular" data between databases (Redshift and PostgreSQL well-supported; BigQuery and MySQL partially), CSV/Parquet files, Pandas DataFrames, and "records directories" (a directory of data files plus JSON metadata describing format and schema; spec in `docs/RECORDS_SPEC.md`). It chooses the fastest available path between source and target, including native bulk load/unload such as Redshift `COPY`/`UNLOAD` via S3.

Intended scope (confirmed by leadership, 2026-09): this is an **internal-only** 14th tool for moving data from anywhere in 14th to anywhere in 14th, **AWS and GCP only**: Redshift, S3, BigQuery, GCS, and PostgreSQL, plus local files and DataFrames for Data Science (DS). It is not for client delivery. Vertica, Google Sheets, Airflow, LastPass and Airbyte support has been removed. 14th uses 1Password, and LastPass is deprecated, so don't reintroduce `lpass`. Google Drive and Sheets are planned for later (see the roadmap); the old Sheets code is still on `main` (`records_mover/records/{sources,targets}/google_sheets.py`). MySQL and Redshift Spectrum stay only if they survive the upgrades. The phased plan is in `docs/ROADMAP.md`, and the baseline findings are in `docs/REVIVAL_ASSESSMENT.md`.

"BlueLabs" and "14th" are the same company. The company was renamed, so the two names are interchangeable. The `bluelabsio` GitHub org, the `bluelabs` config namespace (`get_config('records_mover', 'bluelabs')`), and the "BlueLabs" Redshift/BigQuery test accounts all belong to 14th. Don't rename `bluelabs` identifiers just because of the name change, since they are config and external interfaces.

## Environment and commands

- Python is 3.8/3.9 (CI matrix). `pandas<2` and `numpy<2` are pinned (RM-133). SQLAlchemy is `>=1.4,<2`. Set `SQLALCHEMY_SILENCE_UBER_WARNING=1`, as CI does.
- Dev setup: `./deps.sh` (pyenv virtualenv `records-mover-3.9.1`), or `pip install -e '.[unittest,typecheck]' && pip install -r requirements.txt`.
- Version comes from git tags through `setuptools_scm`, which writes `records_mover/version.py`.

| Task | Command |
|---|---|
| Unit + component tests with combined coverage | `make test` |
| Unit tests only | `make unit` (`ENV=test pytest --cov=records_mover tests/unit`) |
| Single test | `ENV=test pytest tests/unit/records/test_mover.py::TestMover::test_move_from_records_directory_direct` |
| Typecheck | `make typecheck` (mypy on `records_mover`, then on `tests`) |
| Lint | `make flake8` (max line length 100, max complexity 15) |
| Coverage ratchets | `make coverage` / `make typecoverage` |
| Integration tests | `tests/integration/itest --help`, e.g. `./itest postgres`, `./itest --docker all` |
| Live acceptance suite (real Redshift/S3, opt-in) | `make live` with `RECORDS_MOVER_LIVE=1` and the env vars in `tests/integration/live/README.md` |
| Docs | `cd docs && make html` |

Notes:
- `pytest.ini` turns `DeprecationWarning` and `FutureWarning` raised from `records_mover` code into errors.
- The ratchets (`setup.py mypy_ratchet` / `coverage_ratchet`) fail when coverage drops below `metrics/*_high_water_mark`. When coverage goes up, update the high-water-mark file and commit it, because CI checks that those files have no uncommitted changes.
- The dependency set is still pinned to Python 3.9-era versions until Phase 2 of the roadmap. Install with `pip install --prefer-binary --only-binary pandas,numpy,pyarrow,psycopg2-binary -e '.[unittest,typecheck]' -r requirements.txt`. Python 3.12+ cannot install the pinned `pandas<2` yet.
- When passing pytest options from zsh, write them out literally. zsh does not split an unquoted `$VAR`, so options stored in a variable arrive as one malformed argument.
- The live suite (`tests/integration/live/`) defines "works as expected" for the revival. Every statement passes through a SQL/S3 guard, so writes stay within `rm_test_*` tables in the allowed schemas and S3 access stays under the configured prefix. Records-mover gets only 15-minute STS credentials, never long-lived keys, because it embeds AWS credentials in `COPY`/`UNLOAD` SQL. Cleanup always runs.
- Integration tests use docker-compose databases (MySQL, Postgres). The Redshift and BigQuery suites need BlueLabs cloud accounts. CI is GitHub Actions only (`.github/workflows/test.yml`: typecheck, unit/component tests, docs build); CircleCI has been removed, so integration tests currently run only locally.

## Test suites (see `tests/README.md`)

- `tests/unit`: tests one class or module and mocks everything else.
- `tests/component`: tests several classes working together, with mocks only for I/O and sentinel values. Use this suite when refactoring across classes, since unit tests break easily in that case.
- `tests/integration`: runs against real databases and the CLI. The number of cases grows as O(n²) in databases, so add tests here sparingly.

## Architecture

**Entry points.** The public API is `records_mover.sources`, `records_mover.targets`, and `move`, along with `Session` (`records_mover/session.py`). `Session` builds DB engines, S3/GCS clients, and credentials. Credentials come from db-facts and env vars (`records_mover/creds/`), and the session type is inferred from the environment. Anything not in `__all__` or prefixed `_` is not a stable interface.

**Move negotiation (`records_mover/records/mover.py`).** `move(source, target, processing_instructions)` does not dispatch on concrete types. It walks an ordered chain of `isinstance` checks against capability ABCs in `records/sources/base.py` and `records/targets/base.py`, including `SupportsRecordsDirectory`, `SupportsMoveFromRecordsDirectory`, `MightSupportMoveFromFileobjsSource`, `SupportsMoveToRecordsDirectory`, `MightSupportMoveFromTempLocAfterFillingIt`, and `SupportsToDataframesSource`/`SupportsMoveFromDataframes`. It takes the first strategy the source and target both support, from fastest to slowest:
1. The target loads directly from the source's records directory, for example Redshift COPY from S3.
2. The source's file streams go straight to the target.
3. The source unloads into the target's records directory.
4. The source converts to a fileobjs source, then the move recurses.
5. The source unloads to a temporary location, and the target loads from it.
6. The data goes through DataFrames, as the last resort.

To add a source or target, implement the capability mixins that fit and follow `move()` from the top. Records formats (`records_format.py`: delimited/CSV, Parquet, Avro) are negotiated through `can_move_from_format` / `compatible_format` / `known_supported_records_formats`.

**Sources and targets.** The factory classes in `records/sources/factory.py` and `records/targets/factory.py` (`RecordsSources`/`RecordsTargets`) are the public constructors. The table target (`records/targets/table/`) has one module per move strategy.

**CLI (`records/cli.py`, `records/job/`, `cli/`).** `mvrec` subcommands such as `table2table` and `file2table` are generated from pairs of source and target factory methods. Each method's signature and docstring are turned into JSON Schema (`job/schema.py`, `utils/json_schema.py`, using `docstring_parser`), and the schema becomes argparse arguments (`cli/job_config_schema_as_args_parser.py`). Changing a factory method's parameters, type hints, or docstring therefore changes the CLI.

**DB layer (`records_mover/db/`).** `db/factory.py:db_driver()` picks a `DBDriver` subclass by SQLAlchemy engine name (redshift, bigquery, postgresql, mysql, or `GenericDBDriver`). Each driver can provide a `loader()` (`LoaderFromRecordsDirectory`/`LoaderFromFileobj`, in `db/loader.py`) and an `unloader()` (`db/unloader.py`), along with type-mapping hooks (`type_for_integer`, `type_for_floating_point`, and so on) used to generate `CREATE TABLE`. Each database's subpackage turns records-format hints into native bulk load and unload options, for example Redshift COPY/UNLOAD and Postgres `COPY` options in `db/postgres/copy_options/`. `db/postgres/sqlalchemy_postgres_copy.py` is vendored code and is excluded from coverage.

**Delimited hints (`records/delimited/`).** CSV dialect is described by "hints": delimiter, quoting, escaping, compression, date formats, and so on. `sniff.py` infers hints from files, `hints.py`/`validated_records_hints.py` validate them, and `ProcessingInstructions` controls how unsupported hints are handled (fail vs. warn).

**Records schema (`records/schema/`).** `RecordsSchema`/`RecordsSchemaField` form a database-neutral schema with field types, constraints, and statistics. It converts to and from SQLAlchemy (`field/sqlalchemy.py`), pandas, and numpy, and it is used to generate target tables.

**URLs (`records_mover/url/`).** This package abstracts file and directory URLs across local, S3, GCS, and HTTP. `UrlResolver` maps a scheme to an implementation.

**Types.** The package is fully type-annotated (`py.typed`). Local stubs live in `types/stubs` (the mypy `mypy_path`). `setup.cfg` lists modules that mypy should ignore.
