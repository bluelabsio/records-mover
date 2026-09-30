import logging
import json
import sqlalchemy as sa

from records_mover.db.db_type import canonicalize_db_type
from db_facts.db_facts_types import DBFacts
from typing import Union


logger = logging.getLogger(__name__)

# cheap translator between configured types (e.g., from
# cred-service/etc) into which driver to use.
db_driver_for_type = {
    'postgres': 'postgresql',
    # pymysql is pure Python and is known to work correctly with LOAD
    # DATA LOCAL INFILE in SQLAlchemy, which mysqlclient did not as of
    # 2020-04.
    'mysql': 'mysql+pymysql',
}

query_for_type = {
    'mysql': {
        # Please see SECURITY.md for security implications!
        "local_infile": "1"
    },
    # keepalives prevent timeout errors
    'redshift': {'keepalives': '1', 'keepalives_idle': '30'},
}


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


def create_sqlalchemy_url(db_facts: DBFacts) -> Union[str, sa.engine.url.URL]:
    db_type = canonicalize_db_type(db_facts['type'])
    driver = db_driver_for_type.get(db_type, db_type)
    # 'user' is the preferred key, but handle legacy code as well
    # still using 'username'
    username = db_facts.get('username', db_facts.get('user'))
    if driver == 'bigquery':
        if 'bq_service_account_json' in db_facts:
            raise NotImplementedError("pybigquery does not support providing credentials info "
                                      "(service account JSON) directly")

        return create_bigquery_sqlalchemy_url(db_facts)
    else:
        return sa.engine.url.URL.create(  # type: ignore
            drivername=driver,
            username=username,
            password=db_facts['password'],
            host=db_facts['host'],
            port=db_facts['port'],
            database=db_facts['database'],
            query=query_for_type.get(db_type))


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
