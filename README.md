# Records Mover - mvrec

Records mover is a command-line tool and Python library you can
use to move relational data from one place to another.

Relational data here means anything roughly "rectangular" - with
columns and rows.  For example, it supports reading and writing from:

* Databases, including using native high-speed methods of
  import/export of bulk data.  Redshift, Vertica and PostgreSQL are
  well-supported, with some support for BigQuery and MySQL.
* CSV files
* Parquet files (initial support)
* Google Sheets
* Pandas DataFrames
* Records directories - a structured directory of CSV/Parquet/etc
  files containing some JSON metadata about their format and origins.
  Records directories are especially helpful for the ever-ambiguous
  CSV format, where they solve the problem of 'hey, this may be a
  CSV - but what's the schema?  What's the format of the CSV itself?
  How is it escaped?'

Records mover can be exended expand to handle additional databases
and data file types.  Databases are supported by building on top of
their [SQLAlchemy](https://www.sqlalchemy.org/) drivers.  Records
mover is able to auto-negotiate the most efficient way of moving data
from one to the other.

## Example CLI use

Installing:

```sh
pip3 install 'records_mover[cli,postgres,redshift]'
```

Loading a CSV into a database:

```sh
mvrec file2table foo.csv redshiftdb1 myschema1 mytable1
```

Copying a table from a PostgreSQL to a Redshift database:

```sh
mvrec --help
mvrec table2table postgresdb1 myschema1 mytable1 redshiftdb2 myschema2 mytable2
```

Note records mover will automatically build an appropriate `CREATE
TABLE` statement on the target end if the table doesn't already exist.

Note that the connection details for the database names here must be
configured using
[db-facts](https://github.com/bluelabsio/db-facts/blob/master/CONFIGURATION.md).

For more installation notes, see [INSTALL.md](./docs/INSTALL.md).  To
understand the security model here, see [SECURITY.md](./docs/SECURITY.md).

## Example Python library use

First, install records_mover.  We'll also use Pandas, so we'll install
that, too, as well as a driver for Postgres.

```sh
pip3 install records_mover[pandas,postgres]
```

Now we can run this code:

```python
#!/usr/bin/env python3

# Pull in the records-mover library - be sure to run the pip install above first!
from records_mover import sources, targets, move
from pandas import DataFrame
import sqlalchemy
import os

sqlalchemy_url = f"postgresql+psycopg2://username:{os.environ['DB_PASSWORD']}@hostname/database_name"
db_engine = sqlalchemy.create_engine(sqlalchemy_url)

df = DataFrame.from_dict([{'a': 1}]) # or make your own!

source = sources.dataframe(df=df)
target = targets.table(schema_name='myschema',
                       table_name='mytable',
                       db_engine=db_engine)
results = move(source, target)
```

When moving data, the sources supported can be found
[here](./records_mover/records/sources/factory.py), and the
targets supported can be found [here](./records_mover/records/targets/factory.py).

## Advanced Python library use

Here's another example, using some additional features:

* Loading from an existing dataframe.
* Secrets management using
  [db-facts](https://github.com/bluelabsio/db-facts), which is a way
  to configure credentials in YAML files or even fetch them
  dynamically from your secrets store.
* Logging configuration to show the internal processing steps (helpful
  in optimizing performance or debugging issues)

you can use this:

```python
#!/usr/bin/env python3

# Pull in the records-mover library - be sure to run the pip install above first!
from records_mover import Session
from pandas import DataFrame
import sqlalchemy

session = Session()
records = session.records

db_engine = session.get_default_db_engine()

df = DataFrame.from_dict([{'a': 1}]) # or make your own!

source = records.sources.dataframe(df=df)
target = records.targets.table(schema_name='myschema',
                               table_name='mytable',
                               db_engine=db_engine)
results = records.move(source, target)

df = DataFrame.from_dict([{'a': 1}]) # or make your own!

source = sources.dataframe(df=df)
target = targets.table(schema_name='myschema',
                       table_name='mytable',
                       db_engine=db_engine)
results = move(source, target)
```
