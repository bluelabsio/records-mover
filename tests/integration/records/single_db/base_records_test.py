import sys
import unittest
import logging
import time
from records_mover import Session
from sqlalchemy import MetaData
from sqlalchemy.schema import Table
import os
from ..records_database_fixture import RecordsDatabaseFixture
from ..purge_old_test_tables import purge_old_tables


# Note: you're gonna see some of this in the detailed test logging:

# /Users/vincebroz/src/records-mover/records_mover/records/s3_records.py:127:
# ResourceWarning: unclosed <ssl.SSLSocket fd=10,
# family=AddressFamily.AF_INET, type=SocketKind.SOCK_STREAM, proto=6,
# laddr=('REDACTED', 53084), raddr=('REDACTED', 443)>

# Reason seems to be here: https://github.com/boto/boto3/issues/454#issuecomment-324782994

# Logging shouldn't happen in prod, just a result of increased logging
# in test conflicting with boto's idea of connection pooling
# management.

logger = logging.getLogger(__name__)


stdout_handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(message)s', '%H:%M:%S')
stdout_handler.setFormatter(formatter)
logging.getLogger().addHandler(stdout_handler)


class BaseRecordsIntegrationTest(unittest.TestCase):
    def setUp(self):
        # Ensure we're not getting any DWIM behavior out of the CLI
        # job context:
        os.environ['RECORDS_MOVER_SESSION_TYPE'] = 'itest'

        self.resources_dir = os.path.dirname(os.path.abspath(__file__)) + '/../../resources'
        self.session = Session(session_type='env',
                               default_db_creds_name=None,
                               default_aws_creds_name=None)
        self.engine = self.session.get_default_db_engine()
        self.driver = self.session.db_driver(self.engine)
        if self.engine.name == 'bigquery':
            self.schema_name = 'bq_itest'
            # avoid per-table rate limits
        elif self.engine.name == 'mysql':
            self.schema_name = 'mysqlitest'
        else:
            self.schema_name = 'public'
        table_name_prefix = "itest_"
        build_num = os.environ.get("CIRCLE_BUILD_NUM", "local")
        current_epoch = int(time.time())
        self.table_name = f"{table_name_prefix}{build_num}_{current_epoch}"
        self.fixture = RecordsDatabaseFixture(self.engine,
                                              schema_name=self.schema_name,
                                              table_name=self.table_name)
        self.fixture.tear_down()
        purge_old_tables(self.schema_name, table_name_prefix)

        logger.debug("Initialized class!")

        self.meta = MetaData()
        self.records = self.session.records

    def tearDown(self):
        self.session = None
        self.fixture.tear_down()

    def table(self, schema, table):
        return Table(table, self.meta, schema=schema, autoload=True, autoload_with=self.engine)

    def variant_has_header(self, variant):
        return variant in ['csv', 'bigquery']

    def resource_name(self, format_type, variant, hints):
        if hints.get('header-row', self.variant_has_header(variant)):
            return f"{format_type}-{variant}-with-header"
        else:
            return f"{format_type}-{variant}-no-header"

    def has_scratch_bucket(self):
        return os.environ.get('SCRATCH_S3_URL') is not None

    def has_pandas(self):
        try:
            import pandas  # noqa
            logger.info("Just imported pandas")
            return True
        except ModuleNotFoundError:
            logger.info("Could not find pandas")
            return False
