from records_mover.s3_url import S3Url
import unittest
from mock import Mock, patch


class TestS3Url(unittest.TestCase):
    def setUp(self):
        self.mock_boto3_session = Mock(name='boto3_session')
        self.mock_s3_resource = self.mock_boto3_session.resource.return_value
        self.s3_directory_url = S3Url('s3://mybucket/myparent/mychild/',
                                      boto3_session=self.mock_boto3_session)
        self.s3_file_url = S3Url('s3://mybucket/myparent/mychild/mygrandchild',
                                 boto3_session=self.mock_boto3_session)

    def test_directory_file_in_this_directory(self):
        gc = self.s3_directory_url.file_in_this_directory('anothergrandchild')
        self.assertEqual(gc.url, 's3://mybucket/myparent/mychild/anothergrandchild')

    def test_file_file_in_this_directory(self):
        gc = self.s3_file_url.file_in_this_directory('anothergrandchild')
        self.assertEqual(gc.url, 's3://mybucket/myparent/mychild/anothergrandchild')

    def test_purge_directory(self):
        self.mock_s3_resource.meta.client.list_objects.return_value = {
            'Contents': [{
                'Key': 'key_to_delete'
            }]
        }
        self.s3_directory_url.purge_directory()
        self.mock_s3_resource.meta.client.list_objects.\
            assert_called_with(Bucket='mybucket',
                               Prefix='myparent/mychild/')
        self.mock_s3_resource.meta.client.delete_objects.\
            assert_called_with(Bucket='mybucket',
                               Delete={'Objects': [{'Key': 'key_to_delete'}]})

    def test_directory_in_this_directory_from_directory(self):
        out = self.s3_directory_url.directory_in_this_directory('abc')
        self.assertEqual(out.url, 's3://mybucket/myparent/mychild/abc/')

    def test_directory_in_this_directory_from_file(self):
        out = self.s3_file_url.directory_in_this_directory('abc')
        self.assertEqual(out.url, 's3://mybucket/myparent/mychild/abc/')

    @patch('records_mover.url.s3.s3_base_url.secrets')
    def test_temporary_directory(self, mock_secrets):
        self.mock_s3_resource.meta.client.list_objects.return_value = {
            'Contents': [{
                'Key': 'key_to_delete'
            }]
        }
        mock_secrets.token_urlsafe.return_value = '3MNURWKF'
        with self.s3_directory_url.temporary_directory() as d:
            self.assertEqual(d.url, 's3://mybucket/myparent/mychild/3MNURWKF/')
            mock_secrets.token_urlsafe.assert_called_with(8)
