# Maintenance

Packages inside include:

* [records](../records_mover/records/), which is the core API you
  can use to move relational data from one place to another.
* [url](../records_mover/url/), which offers some abstractions
  across different filesystem-like things (e.g., S3/HTTP/local
  filesystems, maybe SFTP in the future)
* [db](../records_mover/db/), which adds some functionality on top of
  SQLAlchemy for various different database types.
* [creds](../records_mover/creds/), which manages credentials and
  other connection details.
* [pandas](../records_mover/pandas/), which adds functionality on top
  of the Pandas data science framework.
* [airflow](../records_mover/airflow/), which helps interface parts
  of this library to DAGS running under Airflow.
* [utils](../records_mover/utils/), which is the usual junk drawer of
  things that haven't grown enough mass to be exported into their own
  package.

Things either labeled private, not in `__all__`, undocumented or with
a prefix of `_` aren't stable interfaces - they can change rapidly.

If you need access to another function/class, please submit an issue
or a PR make it public.  That PR is a good opportunity to talk about
what changes we want to make to the public interface before we make
one--it's a lot harder to change later!

## Development

### Installing development tools

```bash
./deps.sh  # uses pyenv and pyenv-virtualenv
```

### Unit testing

To run the tests in your local pyenv:

```bash
make test
```

### Automated integration testing

All of our integration tests use the `itest` script can can be provided
with the `--docker` flag to run inside docker.

To see details on the tests available, run:

   ```sh
   ./itest --help
   ```

To run all of the test suite locally (takes about 30 minutes):

   ```sh
   ./itest all
   ```

To run the same suite with mover itself in a Docker image:

   ```sh
   ./itest --docker all
   ```

### Common issues with integration tests

```vertica
(vertica_python.errors.InsufficientResources) Severity: b'ERROR', Message: b'Insufficient resources to execute plan on pool general [Request Too Large:Memory(KB) Exceeded: Requested = 5254281, Free = 1369370 (Limit = 1377562, Used = 8192)]', Sqlstate: b'53000', Routine: b'Exec_compilePlan', File: b'/scratch_a/release/svrtar2409/vbuild/vertica/Dist/Dist.cpp', Line: b'1540', Error Code: b'3587', SQL: "         SELECT S3EXPORT( * USING PARAMETERS url='s3://vince-scratch/PA6ViIBMMWk/records.csv', chunksize=5368709120, to_charset='UTF8', delimiter='\x01', record_terminator='\x02')         OVER(PARTITION BEST) FROM public.test_table1     "
```

Try expanding your Docker for Mac memory size to 8G.  Vertica is
memory intensive, even under Docker.

### Documentation

API reference documentation is pushed up to
[readthedocs](https://records-mover.readthedocs.io/en/publish_docs/) by a
GitHub webhook.

To create docs, run this from the `docs/` directory:

* `make html`

To view docs:

* `open build/html/index.html`

### Manual integration testing

There's also a manual records schema JSON functionality
[torture test](tests/integration/table2table/TORTURE.md) available to run -
this may be handy after making large-scale refactors of the records
schema JSON code or when adding load/unload support to a new database
type.

### Semantic versioning

In this house, we use [semantic versioning](http://semver.org) to indicate
when we make breaking changes to interfaces.  If you don't want to live
dangerously, and you are currently using version a.y.z (see setup.py to see
what version we're at) specify your requirement like this in requirements.txt:

records_mover>=a.x.y,<b

This will make sure you don't get automatically updated into the next
breaking change.
