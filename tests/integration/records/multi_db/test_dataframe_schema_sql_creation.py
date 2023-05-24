import unittest
from pandas import DataFrame
from records_mover.records.sources.dataframes import DataframesRecordsSource
from records_mover.db.redshift.redshift_db_driver import RedshiftDBDriver
from records_mover.records.processing_instructions import ProcessingInstructions
from records_mover import Session, set_stream_logging
import logging


class TestDataframeSchemaSqlCreation(unittest.TestCase):
    def test_dataframe_to_int64_and_back_to_object_produces_int_columns(self) -> None:
        # This reproduces a situation found when a user worked around
        # a separate historical Records Mover limitation by doing an
        # unusual cast on their dataframe...and then hit a separate
        # limitation:
        #
        # https://github.com/bluelabsio/records-mover/pull/103
        session = Session()
        engine = session.get_db_engine('demo-itest')
        data = {'Population': [11190846, 1303171035, 207847528]}
        df = DataFrame(data, columns=['Population'])

        df['Population'] = df['Population'].astype("Int64")
        df['Population'] = df['Population'].astype("object")

        source = DataframesRecordsSource(dfs=[df])
        processing_instructions = ProcessingInstructions()
        schema = source.initial_records_schema(processing_instructions)
        driver = RedshiftDBDriver(db=engine)
        schema_sql = schema.to_schema_sql(driver=driver,
                                          schema_name='my_schema_name',
                                          table_name='my_table_name')
        expected_schema_sql = """
CREATE TABLE my_schema_name.my_table_name (
\t"Population" INTEGER
)

"""
        self.assertEqual(schema_sql, expected_schema_sql)


if __name__ == '__main__':
    set_stream_logging(level=logging.DEBUG)
    logging.getLogger('botocore').setLevel(logging.INFO)
    logging.getLogger('boto3').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)

    unittest.main()
