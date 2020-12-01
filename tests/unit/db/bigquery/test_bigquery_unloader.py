import unittest

from records_mover.db.bigquery.unloader import BigQueryUnloader
from records_mover.records.records_format import (
    DelimitedRecordsFormat, AvroRecordsFormat
)
from records_mover.db.errors import NoTemporaryBucketConfiguration
from mock import MagicMock, Mock
from unittest.mock import ANY


class TestBigQueryUnloader(unittest.TestCase):
    def test_can_unload_format_avro_true(self):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        mock_gcs_temp_base_loc = MagicMock(name='gcs_temp_base_loc')
        big_query_unloader = BigQueryUnloader(db=mock_db, url_resolver=mock_url_resolver,
                                              gcs_temp_base_loc=mock_gcs_temp_base_loc)
        avro_format = AvroRecordsFormat()
        self.assertTrue(big_query_unloader.can_unload_format(avro_format))

    def test_can_unload_format_delimited_false(self):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        mock_gcs_temp_base_loc = MagicMock(name='gcs_temp_base_loc')
        big_query_unloader = BigQueryUnloader(db=mock_db, url_resolver=mock_url_resolver,
                                              gcs_temp_base_loc=mock_gcs_temp_base_loc)
        delimited_format = DelimitedRecordsFormat()
        self.assertFalse(big_query_unloader.can_unload_format(delimited_format))

    def test_can_unload_to_scheme_gs_true(self):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        mock_gcs_temp_base_loc = MagicMock(name='gcs_temp_base_loc')
        big_query_unloader = BigQueryUnloader(db=mock_db, url_resolver=mock_url_resolver,
                                              gcs_temp_base_loc=mock_gcs_temp_base_loc)
        self.assertTrue(big_query_unloader.can_unload_to_scheme('gs'))

    def test_can_unload_to_scheme_other_with_temp_bucket_true(self):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        mock_gcs_temp_base_loc = MagicMock(name='gcs_temp_base_loc')
        big_query_unloader = BigQueryUnloader(db=mock_db, url_resolver=mock_url_resolver,
                                              gcs_temp_base_loc=mock_gcs_temp_base_loc)
        self.assertTrue(big_query_unloader.can_unload_to_scheme('blah'))

    def test_can_unload_to_scheme_other_with_no_temp_bucket_true(self):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        mock_gcs_temp_base_loc = None
        big_query_unloader = BigQueryUnloader(db=mock_db, url_resolver=mock_url_resolver,
                                              gcs_temp_base_loc=mock_gcs_temp_base_loc)
        self.assertFalse(big_query_unloader.can_unload_to_scheme('blah'))

    def test_known_supported_records_formats_for_unload(self):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        mock_gcs_temp_base_loc = MagicMock(name='gcs_temp_base_loc')
        big_query_unloader = BigQueryUnloader(db=mock_db, url_resolver=mock_url_resolver,
                                              gcs_temp_base_loc=mock_gcs_temp_base_loc)
        self.assertEqual([type(format)
                          for format in
                          big_query_unloader.known_supported_records_formats_for_unload()],
                         [AvroRecordsFormat])

    def test_temporary_unloadable_directory_loc_raises(self):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        mock_gcs_temp_base_loc = None
        big_query_unloader = BigQueryUnloader(db=mock_db, url_resolver=mock_url_resolver,
                                              gcs_temp_base_loc=mock_gcs_temp_base_loc)
        with self.assertRaises(NoTemporaryBucketConfiguration):
            with big_query_unloader.temporary_unloadable_directory_loc():
                pass

    def test_temporary_unloadable_directory_loc(self):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        mock_gcs_temp_base_loc = MagicMock(name='gcs_temp_base_loc')
        big_query_unloader = BigQueryUnloader(db=mock_db, url_resolver=mock_url_resolver,
                                              gcs_temp_base_loc=mock_gcs_temp_base_loc)
        with big_query_unloader.temporary_unloadable_directory_loc() as temp_loc:
            self.assertEqual(temp_loc,
                             mock_gcs_temp_base_loc.temporary_directory.return_value.__enter__.
                             return_value)

    def test_unload(self):
        mock_db = Mock(name='mock_db')
        mock_url_resolver = MagicMock(name='mock_url_resolver')
        mock_gcs_temp_base_loc = MagicMock(name='gcs_temp_base_loc')
        big_query_unloader = BigQueryUnloader(db=mock_db, url_resolver=mock_url_resolver,
                                              gcs_temp_base_loc=mock_gcs_temp_base_loc)
        mock_schema = 'myproject.mydataset'
        mock_table = 'mytable'
        mock_unload_plan = Mock(name='unload_plan')
        mock_unload_plan.records_format = AvroRecordsFormat()
        mock_directory = Mock(name='directory')
        mock_directory.scheme = 'gs'
        big_query_unloader.unload(schema=mock_schema,
                                  table=mock_table,
                                  unload_plan=mock_unload_plan,
                                  directory=mock_directory)
        mock_connection = mock_db.engine.raw_connection.return_value.connection
        mock_client = mock_connection._client
        mock_destination_uri = mock_directory.loc.file_in_this_directory.return_value
        mock_url = mock_destination_uri.url
        mock_directory.loc.file_in_this_directory.assert_called_with('output.avro')
        mock_client.extract_table.assert_called_with('myproject.mydataset.mytable',
                                                     mock_url,
                                                     job_config=ANY)
        mock_directory.save_preliminary_manifest.assert_called_with()
