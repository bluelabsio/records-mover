import os
from sqlalchemy.engine import Engine
from records_mover.db.connect import engine_from_db_facts
from db_facts.db_facts_types import DBFacts
from typing import Dict, Optional, TYPE_CHECKING
if TYPE_CHECKING:
    # http://mypy.readthedocs.io/en/latest/common_issues.html#import-cycles
    from . import Session  # noqa


def db_facts_from_env() -> DBFacts:
    db_facts: Dict[str, Optional[str]]
    if os.environ.get('DB_TYPE') == 'bigquery':
        db_facts = {
            'type': os.environ['DB_TYPE']
        }
        if 'BQ_DEFAULT_PROJECT_ID' in os.environ:
            db_facts['bq_default_project_id'] = os.environ['BQ_DEFAULT_PROJECT_ID']
        if 'BQ_DEFAULT_DATASET_ID' in os.environ:
            db_facts['bq_default_dataset_id'] = os.environ['BQ_DEFAULT_DATASET_ID']
        if 'BQ_SERVICE_ACCOUNT_JSON' in os.environ:
            db_facts['bq_service_account_json'] = os.environ['BQ_SERVICE_ACCOUNT_JSON']
    else:
        db_facts = {
            'host': os.environ.get('DB_HOST'),
            'database': os.environ.get('DB_DATABASE'),
            'port': os.environ.get('DB_PORT'),
            'user': os.environ.get('DB_USERNAME'),
            'password': os.environ.get('DB_PASSWORD'),
            'type': os.environ.get('DB_TYPE')
        }
        redshift_base_url_values = {
            name.lower(): value
            for name, value in os.environ.items()
            if name.startswith('REDSHIFT_SPECTRUM_BASE_URL_')
        }

        db_facts.update(redshift_base_url_values)

        if None in db_facts.values():
            raise NotImplementedError("Please run with with-db or set DB_* environment variables")
    return db_facts  # type: ignore


def db_engine(session: 'Session') -> Engine:
    db_facts = db_facts_from_env()
    return engine_from_db_facts(db_facts)
