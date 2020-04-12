# Installing Records Mover

You can install records-mover with a number of 'extras'.  See
[setup.py](./setup.py) and the `extras_require` section for a full
list.

Example:

* `pip3 install records-mover` - Install minimal version.  Not able to
  connect to databases, deal with dataframes either as input, etc.
* `pip3 install records-mover[gsheets,pandas]` - Minimal install plus API
  libraries to access Google Sheets and Pandas DataFrames.
* `pip3 install records-mover[cli,redshift-binary,pandas,parquet]` -
  Install enough things to be able to use the `mvrec` command line,
  talk to the Redshift database, and use Parquet internally and/or as
  an input/output.  Installs `pandas`, `psycopg2-binary` and
  `pyarrow`, among others.

  You might consider `redshift-source` if you plan on using the
  library because of the `psycopg2-binary` risk below.

In generally you'll need to spell out which databases you want to be
able to move between (check `setup.py` for the list available.

## Why this is complicated

Records mover relies on a number of external libraries.  Each database
comes with its own driver, some of which depend on binary libraries
which may need to be installed in your OS.

Indeed, some of those are even difficult to install.  Some examples:

### pandas

Only when installing with `pip3 install 'records-mover[movercli]'`
will you get pandas installed by default.

Pandas a large dependency which is needed in cases where we need to
process data locally.  If you are using cloud-native import/export
functionality only, you shouldn't need it and can avoid the bloat.

### psycopg2

psycopg2 is a library used for access to both Redshift and PostgreSQL databases.

The project is
[dealing](https://www.postgresql.org/message-id/CA%2Bmi_8bd6kJHLTGkuyHSnqcgDrJ1uHgQWvXCKQFD3tPQBUa2Bw%40mail.gmail.com)
[with](https://www.psycopg.org/articles/2018/02/08/psycopg-274-released/)
a thorny compatibility issue with native code and threading.  They've
published three separate versions of their library to PyPI as a
result:

* `psycopg2` - requires local compilation, and as such you need certain
  tools and maybe configuration set up.  This is the hardest one to
  install as a result.
* `psycopg2-binary` - pre-compiled version that might have threading
  issues if you try to use it in a multi-threaded environment with
  other code that might be using libssl from a different source.
* `psycopg2cffi` - The version to use if you use `pypy`

If you are using the mvrec command line only, you can use `pip3
install 'records-mover[movercli]` and it just uses `psycopg2-binary`.

### pyarrow

`pyarrow` is a Python wrapper around the Apache Arrow native library.
It's used by records mover to manipulate Parquet files locally.  The
Apache Arrow native library can require build tools to install and is
large; if you don't need to deal with Parquet files in the local
environment you can work without it.
