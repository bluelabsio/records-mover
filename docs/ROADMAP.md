# Roadmap: records-mover revival

**Goal:** an internal 14th tool that moves tabular data from anywhere in 14th to anywhere in 14th, across AWS and GCP. The primary users are Data Science (DS). The tool is not for client delivery.

This plan builds on [REVIVAL_ASSESSMENT.md](./REVIVAL_ASSESSMENT.md), which covers the baseline state, test results, and known bugs.

## Guiding rule

> If it survives the package updates **and** still works as expected, keep it. Otherwise, burn it to the ground.

Two definitions make this rule usable:

- **"Works as expected"** means it passes the **live acceptance suite** (Phase 1): real data moved between real 14th resources, with row counts, values, and column types verified. Mocked unit tests passing is not enough.
- **Time box:** each piece that breaks in a phase gets a fixed budget (default: 1 engineer-day per backend per phase). If it isn't fixed within that budget, it is deleted along with its tests, extras, and docs.

## Target support matrix (end state)

| Location | Kind | Source | Target | Fast path |
|---|---|---|---|---|
| Redshift | AWS warehouse | ✅ | ✅ | `UNLOAD`/`COPY` via S3, using `IAM_ROLE` |
| S3 | AWS object store | ✅ | ✅ | records directories, CSV, Parquet |
| BigQuery | GCP warehouse | ✅ | ✅ | load and extract jobs via GCS |
| GCS | GCP object store | ✅ | ✅ | records directories, CSV, Parquet |
| PostgreSQL (e.g. RDS) | AWS database | ✅ | ✅ | `COPY` |
| Local file / DataFrame | DS interface | ✅ | ✅ | pandas / pyarrow |

Every source must be able to move to every target. Cross-cloud moves (Redshift ↔ BigQuery, S3 ↔ GCS) go through a records directory in the target cloud's object store.

**Kept only if they survive the upgrades:** MySQL (RDS) and Redshift Spectrum.

**Removed in Phase 1 (outside AWS/GCP, or not a data location):** Vertica, Google Sheets, the Airflow hooks and credentials, LastPass credentials, and the Airbyte feature flag.

## Phases

### Phase 0: Baseline and decisions ✅ (done 2026-09-29)

- Assessed the repo on Python 3.9 and 3.12 (see the assessment).
- Ran live Redshift smoke tests on both the non-S3 path and the S3 `COPY`/`UNLOAD` path.
- Confirmed the scope with leadership: internal only, with DS as the primary users.
- Wrote this roadmap.

**Exit criteria:** everyone understands the baseline, and the scope is agreed. ✅

### Phase 1: Prune and stabilize on the current stack

The goal is a green, smaller codebase before any upgrade, plus a test suite that defines "works".

1. **Burn the out-of-scope code:** Vertica, Google Sheets, Airflow, LastPass, and Airbyte. This includes their code, tests, stubs, dependency extras, CLI subcommands, CI jobs, and docs.
2. **Quick fixes:**
   - `mypy_extensions.TypedDict` → `typing_extensions`.
   - Clear the 22 mypy errors.
   - Fix the JSON schema writer crash on numpy column names.
   - Make Parquet export fail loudly instead of writing a 0-byte file.
3. **Live acceptance suite:** move the guarded smoke tests into `tests/integration/live/`. The suite is opt-in and uses env-configured schema, bucket, and prefix, with the SQL/S3 scope guard, temporary credentials, and cleanup that always runs.

**Exit criteria:**

- `make typecheck`, unit, and component tests pass on Python 3.9 with **no** warning overrides.
- The live suite passes against Redshift and S3.
- No references to the removed backends remain.

### Phase 2: Modernize the runtime (first survival test)

1. Move to Python 3.11+ and target 3.12.
2. Remove the `pandas<2`, `numpy<2`, and `pytest<8.2` caps. Update pandas APIs, for example `line_terminator` → `lineterminator`, and the `read_csv` date options.
3. Replace `setup.py` and its `distutils` ratchet commands with `pyproject.toml`-only packaging. Move the coverage ratchets to a script.
4. Replace the abandoned dependencies: `odictliteral` → `dict`, `timeout_decorator` → built-in timeouts, `config-resolver`, and the `google` package.
5. Apply the survival rule to MySQL and Spectrum.

**Exit criteria:** everything in Phase 1's exit criteria still holds on Python 3.12 with current pandas and numpy.

### Phase 3: SQLAlchemy 2 and the database layer

1. Migrate to SQLAlchemy 2.x: 2.0-style execution and explicit transactions. Drop `sqlalchemy-stubs`.
2. Upgrade the drivers to `sqlalchemy-redshift` 1.x and current `sqlalchemy-bigquery`.
3. Fix connection lifetime. Stop holding long-lived `db_conn` connections that leave `AccessShareLock`s behind (see the assessment).
4. Fix reflection cost: cache `get_columns`, or query `information_schema` directly for a single table. Right now each call costs about 7 seconds on Redshift.
5. Fix type fidelity: DataFrame `Decimal`/`date` values of the `object` dtype currently land in `varchar`; they should map to `numeric`/`date`.

**Exit criteria:**

- The live suite passes on SQLAlchemy 2.
- Table → DataFrame on a 1,000-row Redshift table takes under 5 seconds.
- No lock is held after reads.

### Phase 4: AWS/GCP auth and security

1. Support Redshift `COPY`/`UNLOAD` with `IAM_ROLE` and make it the default, so credentials are never embedded in SQL.
2. Make GCP auth use Application Default Credentials or service accounts for BigQuery and GCS.
3. Handle connections through a SQL access proxy: TLS with `sslmode=verify-full` and the system/certifi CA bundle, plus a documented identity model.
4. Document the credentials and config setup for DS. Decide whether db-facts stays or is replaced by a simpler config.

**Exit criteria:**

- The live suite runs with IAM-role `COPY`/`UNLOAD` and GCP ADC.
- No long-lived keys appear in any SQL or log.

### Phase 5: Full AWS ↔ GCP matrix

1. Bring BigQuery and GCS up to the same live-suite coverage as Redshift and S3.
2. Implement and verify the cross-cloud moves: Redshift ↔ BigQuery, S3 ↔ GCS, and Postgres ↔ both warehouses.
3. Implement the Parquet export path, which is currently `NotImplementedError`, and prefer Parquet for cross-cloud moves where both ends support it.
4. Benchmark the fast paths on realistic DS volumes, for example 10M rows.

**Exit criteria:**

- Every cell of the target support matrix passes the live suite.
- The benchmarks are recorded in the docs.

### Phase 6: DS usability and release (end state)

1. Clean up the CLI (`mvrec`) for DS workflows and write examples for common moves.
2. Consolidate CI onto GitHub Actions: unit and component tests on each PR, the live suite on a schedule or manual trigger with scoped credentials, and retire CircleCI.
3. Package and distribute internally: versioning and publishing, either to an internal index or pinned git installs.
4. Write docs: an install guide, a credentials guide, the support matrix, and a troubleshooting page.

**Exit criteria (the goal):** a DS user can install the tool and move data between any two 14th AWS/GCP locations in the matrix, with one command or one Python call, using their own scoped credentials.

## Execution notes

- **Branching:** each phase is a branch off `main` and merges when its exit criteria are met. Work stays local until the team agrees to push.
- **Model allocation for AI-assisted work:**
  - Mechanical, well-specified edits (renames, deletions, stub fixes) go to smaller, faster models.
  - Multi-file changes that need judgement (removing backends, bug fixes, test harnesses) go to mid-tier models.
  - Planning, review, cross-cutting design, and anything touching live credentials stay with the most capable model.
- **Live resources:** until CI credentials exist, the live suite runs only against the agreed dev scope: a development Redshift target, personal scratch schemas, and a test prefix in a personal scratch bucket.
