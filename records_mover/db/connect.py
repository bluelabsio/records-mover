import logging
import json
import sqlalchemy as sa
from urllib.parse import quote_plus
from records_mover.creds.lpass import db_facts_from_lpass
from records_mover.db.db_type import canonicalize_db_type
from db_facts.db_facts_types import DBFacts
from typing import Union


logger = logging.getLogger(__name__)

# cheap translator between configured types (e.g., from
# cred-service/LastPass/etc) into which driver to use.
db_driver_for_type = {
    'postgres': 'postgresql',
    # pymysql is pure Python and is known to work correctly with LOAD
    # DATA LOCAL INFILE in SQLAlchemy, which mysqlclient did not as of
    # 2020-04.
    'mysql': 'mysql+pymysql',
    # vertica_python has the advantage of being pure Python and an
    # offering directly from Vertica:
    # https://github.com/vertica/vertica-python
    'vertica': 'vertica+vertica_python',
}

odbc_driver_for_type = {
    'vertica': 'vertica+pyodbc',
}

query_for_type = {
    'mysql': {
        # Please see SECURITY.md for security implications!
        "local_infile": True
    },
}


def create_vertica_odbc_sqlalchemy_url(db_facts: DBFacts) -> str:
    # Vertica wants the port in its ODBC connect string as a separate
    # parameter called "Port":
    #
    # https://my.vertica.com/docs/7.1.x/HTML/Content/Authoring/
    #   ConnectingToHPVertica/ClientODBC/DSNParameters.htm
    #
    # Unfortunately, sqlalchemy wants to provide port as part of the
    # 'Server' argument:
    #
    # https://github.com/zzzeek/sqlalchemy/blob/master/lib/sqlalchemy/
    #    connectors/pyodbc.py#L84
    #
    # As a result, we can't use the standard sqlalchemy URL format.

    db_url = ("Driver=HPVertica;"
              "SERVER={host};"
              "DATABASE={database};"
              "PORT={port};"
              "UID={user};"
              "PWD={password};"
              "CHARSET=UTF8;").\
              format(host=db_facts['host'],
                     database=db_facts['database'],
                     port=db_facts['port'],
                     user=db_facts['user'],
                     password=db_facts['password'])
    db_url = quote_plus(db_url)
    return "vertica+pyodbc:///?odbc_connect={}".format(db_url)


def create_bigquery_sqlalchemy_url(db_facts: DBFacts) -> str:
    "Create URL compatible with https://github.com/mxmzdlv/pybigquery"

    default_project_id = db_facts.get('bq_default_project_id')
    default_dataset_id = db_facts.get('bq_default_dataset_id')
    url = 'bigquery://'
    if default_project_id is not None:
        url += default_project_id
        if default_dataset_id is not None:
            url += '/'
            url += default_dataset_id
    return url


def create_bigquery_db_engine(db_facts: DBFacts) -> sa.engine.Engine:
    service_account_json = db_facts.get('bq_service_account_json')
    credentials_info = None
    if service_account_json is not None:
        credentials_info = json.loads(service_account_json)
        logger.info(f"Logging into BigQuery as {credentials_info['client_email']}")
    else:
        logger.info("Found no service account info for BigQuery, using local creds")
    url = create_bigquery_sqlalchemy_url(db_facts)
    return sa.engine.create_engine(url, credentials_info=credentials_info)


def create_sqlalchemy_url(db_facts: DBFacts,
                          prefer_odbc: bool = False) -> Union[str, sa.engine.url.URL]:
    db_type = canonicalize_db_type(db_facts['type'])
    driver = db_driver_for_type.get(db_type, db_type)
    if prefer_odbc:
        driver = odbc_driver_for_type.get(db_type, driver)
    # 'user' is the preferred key, but handle legacy code as well
    # still using 'username'
    username = db_facts.get('username', db_facts.get('user'))  # type: ignore
    if driver == 'vertica+pyodbc':
        return create_vertica_odbc_sqlalchemy_url(db_facts)
    elif driver == 'bigquery':
        if 'bq_service_account_json' in db_facts:
            raise NotImplementedError("pybigquery does not support providing credentials info "
                                      "(service account JSON) directly")

        return create_bigquery_sqlalchemy_url(db_facts)
    else:
        return sa.engine.url.URL(drivername=driver,
                                 username=username,
                                 password=db_facts['password'],
                                 host=db_facts['host'],
                                 port=db_facts['port'],
                                 database=db_facts['database'],
                                 query=query_for_type.get(db_type))


def engine_from_lpass_entry(lpass_entry_name: str) -> sa.engine.Engine:
    db_facts = db_facts_from_lpass(lpass_entry_name)
    return engine_from_db_facts(db_facts)


def engine_from_db_facts(db_facts: DBFacts) -> sa.engine.Engine:
    db_type = canonicalize_db_type(db_facts['type'])
    driver = db_driver_for_type.get(db_type, db_type)
    if driver == 'bigquery':
        # without writing creds to a temp file, pybigquery doesn't
        # support specifying service account creds in a URL - so let's
        # use create_engine() instead of creating a URL just in case.
        return create_bigquery_db_engine(db_facts)
    else:
        db_url = create_sqlalchemy_url(db_facts)
        return sa.create_engine(db_url)
