import unittest
from records_mover.db.redshift.loader import RedshiftLoader
from records_mover.db.errors import NoTemporaryBucketConfiguration
from mock import Mock


class TestRedshiftLoaderTemporaryBucket(unittest.TestCase):
    def test_temporary_s3_directory_loc_no_bucket(self):
        mock_db = Mock(name='db')
        mock_meta = Mock(name='meta')

        redshift_loader =\
            RedshiftLoader(db=mock_db,
                           meta=mock_meta,
                           s3_temp_base_loc=None)

        with self.assertRaises(NoTemporaryBucketConfiguration):
            with redshift_loader.temporary_s3_directory_loc():
                pass
