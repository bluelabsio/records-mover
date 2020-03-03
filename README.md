# Records Mover - mvrec

Records Mover is a command-line tool and Python library you can
use to move relational data from one place to another.

Relational data here means anything roughly "rectangular" - with
columns and rows.  For example, CSV it supports reading and writing
data in:

* Databases, including using native high-speed methods of
  import/export of bulk data.  Redshift and Vertica are
  well-supported, with some support for BigQuery and PostgreSQL.
* Google Sheets
* Pandas DataFrames
* CSV files, either alone or in a records directory - a structured
  directory of CSV/Parquet/etc files containing some JSON metadata
  about their format and origins.  Records directories are especially
  helpful for the ever-ambiguous CSV format, where they solve the
  problem of 'hey, this may be a CSV - but what's the schema?  What's
  the format of the CSV itself?  How is it escaped?'

The record mover can be exended expand to handle additional database
and data file types by building on top of their
[SQLAlchemy](https://www.sqlalchemy.org/) drivers, and is able to
auto-negotiate the most efficient way of moving data from one to the
other.

Example CLI use:

```sh
pip3 install 'records_mover[movercli]'
mvrec --help
mvrec table2table mydb1 myschema1 mytable1 mydb2 myschema2 mytable2
```

For more installation notes, see [INSTALL.md](./INSTALL.md)

Note that the connection details for the database names here must be
configured using
[db-facts](https://github.com/bluelabsio/db-facts/blob/master/CONFIGURATION.md).

Example Python library use:

First, install records_mover.  We'll also use Pandas, so we'll install
that, too:

```sh
pip3 install records_mover pandas
```

Now we can run this code:

```python
#!/usr/bin/env python3

# Pull in the job lib library - be sure to run the pip install above first!
from records_mover import Session
from pandas import DataFrame

session = Session()
records = session.records

# This is a SQLAlchemy database engine.
#
# You can instead call session.get_db_engine('cred name').
#
# On your laptop, 'cred name' is the same thing passed to dbcli (mapping to
# something in your db-facts config).
#
# In Airflow, 'cred name' maps to the connection ID in the admin Connnections UI.
#
# Or you can build your own and pass it in!
db_engine = session.get_default_db_engine()

df = DataFrame.from_dict([{'a': 1}]) # or make your own!

source = records.sources.dataframe(df=df)
target = records.targets.table(schema_name='myschema',
                               table_name='mytable',
                               db_engine=db_engine)
results = records.move(source, target)
```

When moving data, the sources supported can be found
[here](./records_mover/records/sources/factory.py), and the
targets supported can be found [here](./records_mover/records/targets/factory.py).
