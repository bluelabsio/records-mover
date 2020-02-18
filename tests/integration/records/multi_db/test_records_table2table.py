from records_mover.db.quoting import quote_schema_and_table
from records_mover.job_context import pull_job_context
from records_mover.records.existing_table_handling import ExistingTableHandling
import logging
import time
import unittest
from ..records_database_fixture import RecordsDatabaseFixture
from ..table_validator import RecordsTableValidator
from ..purge_old_test_tables import purge_old_tables

logger = logging.getLogger(__name__)

CURRENT_EPOCH = int(time.time())
TARGET_TABLE_NAME_PREFIX = "itest_target"
TARGET_TABLE_NAME = f'{TARGET_TABLE_NAME_PREFIX}_{CURRENT_EPOCH}'

DB_TYPES = ['vertica', 'redshift', 'bigquery', 'postgres']

DB_NAMES = {
    'vertica': 'dockerized-vertica',
    'redshift': 'demo-itest',
    'bigquery': 'bltoolsdevbq-bq_itest',
    'postgres': 'dockerized-postgres',
}


def schema_name(db_name):
    if db_name == 'demo-itest':
        return 'itest'
    elif db_name == 'dockerized-vertica':
        return 'public'
    elif db_name == 'dockerized-postgres':
        return 'public'
    elif db_name == 'bltoolsdevbq-bq_itest':
        return 'bq_itest'
    else:
        raise NotImplementedError('Teach me how to determine a schema name for ' + db_name)


class RecordsMoverTable2TableIntegrationTest(unittest.TestCase):
    #
    # actual test methods are defined dynamically after the class
    # definition below
    #

    @classmethod
    def setUpClass(cls):
        # only needed once per run to clear out old test tables from
        # failed runs and keep our schemas from getting cluttered.
        for db_name in DB_NAMES.values():
            purge_old_tables(schema_name(db_name), TARGET_TABLE_NAME_PREFIX,
                             db_name=db_name)

    def move_and_verify(self, source_dbname, target_dbname, variant_used_internally):
        job_context = pull_job_context('itest')
        records = job_context.records
        targets = records.targets
        sources = records.sources
        move = records.move
        source_engine = job_context.get_db_engine(source_dbname)
        target_engine = job_context.get_db_engine(target_dbname)
        source_schema_name = schema_name(source_dbname)
        target_schema_name = schema_name(target_dbname)
        source_table_name = f'itest_source'
        records_database_fixture = RecordsDatabaseFixture(source_engine,
                                                          source_schema_name,
                                                          source_table_name)
        records_database_fixture.tear_down()
        records_database_fixture.bring_up()

        existing = ExistingTableHandling.DROP_AND_RECREATE
        source = sources.table(schema_name=source_schema_name,
                               table_name=source_table_name,
                               db_engine=source_engine)
        target = targets.table(schema_name=target_schema_name,
                               table_name=TARGET_TABLE_NAME,
                               db_engine=target_engine,
                               existing_table_handling=existing)
        out = move(source, target)
        # redshift doesn't give reliable info on load results, so this
        # will be None or 1
        self.assertNotEqual(0, out.move_count)
        validator = RecordsTableValidator(target_engine,
                                          source_data_db_engine=source_engine)
        validator.validate(variant=variant_used_internally,
                           schema_name=target_schema_name,
                           table_name=TARGET_TABLE_NAME)

        quoted_target = quote_schema_and_table(target_engine, target_schema_name, TARGET_TABLE_NAME)
        sql = f"DROP TABLE {quoted_target}"
        target_engine.execute(sql)

        records_database_fixture.tear_down()


def create_test_func(source_name, target_name):
    def source2target(self):
        self.move_and_verify(source_name, target_name,
                             variant_used_internally='vertica')
    return source2target


if __name__ == '__main__':
    for source in DB_TYPES:
        for target in DB_TYPES:
            variant_used_internally = 'bluelabs'
            if 'vertica' in [source, target]:
                variant_used_internally = 'vertica'

            source_name = DB_NAMES[source]
            target_name = DB_NAMES[target]
            f = create_test_func(source_name, target_name)
            func_name = f"test_{source}2{target}"

            setattr(RecordsMoverTable2TableIntegrationTest,
                    func_name,
                    f)

    unittest.main()
