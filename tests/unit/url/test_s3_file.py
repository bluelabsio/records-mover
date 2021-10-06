from records_mover.url.s3.s3_file_url import S3FileUrl, SMART_OPEN_USE_SESSION
from mock import patch, Mock, MagicMock, ANY
import unittest


class TestS3FileUrl(unittest.TestCase):
    def setUp(self):
        self.mock_S3Url = Mock(name='S3Url')
        self.mock_boto3_session = Mock(name='boto3_session')
        self.s3_file_url = S3FileUrl('s3://bucket/topdir/bottomdir/file',
                                     S3Url=self.mock_S3Url,
                                     boto3_session=self.mock_boto3_session)
        self.mock_s3_resource = self.mock_boto3_session.resource.return_value
        self.mock_s3_client = self.mock_boto3_session.client.return_value
        if SMART_OPEN_USE_SESSION:
            self.open_boto_args = {"session": self.mock_boto3_session}
        else:
            self.open_boto_args = {"client": self.mock_s3_client}

    def test_aws_creds(self):
        self.assertEqual(self.s3_file_url.aws_creds(),
                         self.mock_boto3_session.get_credentials.return_value.
                         get_frozen_credentials.return_value)

    def test_filename(self):
        self.assertEqual('file', self.s3_file_url.filename())

    def test_upload_fileobj(self):
        mock_fileobj = Mock(name='fileobj')
        self.s3_file_url.upload_fileobj(mock_fileobj)
        self.mock_s3_client.upload_fileobj.assert_called_with(Bucket='bucket',
                                                              Callback=ANY,
                                                              Fileobj=mock_fileobj,
                                                              Key='topdir/bottomdir/file')
        callback = self.mock_s3_client.upload_fileobj.mock_calls[0][2]['Callback']
        callback(1)
        callback(2)
        self.assertEqual(callback.length, 3)

    @patch('records_mover.url.s3.s3_file_url.super')
    def test_upload_fileobj_other_write_mode(self, mock_super):
        mock_fileobj = Mock(name='fileobj')
        mock_mode = Mock(name='mode', spec=str)
        self.s3_file_url.upload_fileobj(mock_fileobj, mode=mock_mode)
        mock_base_class = mock_super.return_value
        mock_base_class.upload_fileobj.assert_called_with(mock_fileobj, mode=mock_mode)

    def test_rename_to(self):
        mock_new_loc = Mock(name='new_loc', spec=S3FileUrl)
        mock_new_loc.bucket = 'bucket'
        mock_new_loc.key = 'newfile'
        mock_new_loc.url = 's3://bucket/newfile'
        out = self.s3_file_url.rename_to(mock_new_loc)
        self.mock_s3_resource.Object.return_value.copy_from.assert_called_with(CopySource={
            'Bucket': 'bucket',
            'Key': 'topdir/bottomdir/file'
        })
        self.mock_s3_resource.Object.return_value.delete.assert_called_with()
        self.assertEqual(out, mock_new_loc)

    @patch('records_mover.url.s3.s3_file_url.s3_open')
    def test_wait_to_exist_exists_already(self, mock_s3_open):
        self.s3_file_url.wait_to_exist()

        mock_s3_open.assert_called_with(bucket_id='bucket',
                                        key_id='topdir/bottomdir/file',
                                        mode='rb',
                                        **self.open_boto_args)

    @patch('records_mover.url.s3.s3_file_url.s3_open')
    def test_wait_to_exist_one_loop(self, mock_s3_open):
        mock_file = MagicMock(name='file')
        err = ("'b0KD9AkG7XA/_manifest' does not exist in the bucket 'vince-scratch', "
               "or is forbidden for access")
        mock_s3_open.side_effect = [
            ValueError(err),
            mock_file
        ]
        self.s3_file_url.wait_to_exist()

        mock_s3_open.assert_called_with(bucket_id='bucket',
                                        key_id='topdir/bottomdir/file',
                                        mode='rb',
                                        **self.open_boto_args)

    @patch('records_mover.url.s3.s3_file_url.s3_open')
    def test_open_other_valueerror_passes_through(self, mock_s3_open):
        mock_s3_open.side_effect = ValueError('something')
        with self.assertRaises(ValueError):
            self.s3_file_url.open()

    def test_download_fileobj(self):
        mock_fileobj = Mock(name='fileobj')
        self.s3_file_url.download_fileobj(mock_fileobj)
        self.mock_s3_client.download_fileobj.assert_called_with(Fileobj=mock_fileobj,
                                                                Bucket='bucket',
                                                                Key='topdir/bottomdir/file')

    def test_store_string(self):
        mock_contents = Mock(name='contents', spec=str)
        mock_object = self.mock_s3_resource.Object.return_value
        self.s3_file_url.store_string(mock_contents)
        self.mock_s3_resource.Object.assert_called_with('bucket',
                                                        'topdir/bottomdir/file')
        mock_object.put.assert_called_with(Body=mock_contents)

    def test_delete(self):
        self.s3_file_url.delete()
        mock_object = self.mock_s3_resource.Object.return_value
        self.mock_s3_resource.Object.assert_called_with('bucket',
                                                        'topdir/bottomdir/file')
        mock_object.delete.assert_called_with()

    def test_size(self):
        mock_content_length = Mock(name='content_length')
        mock_response = {'ContentLength': mock_content_length}
        self.mock_s3_client.head_object.return_value = mock_response
        out = self.s3_file_url.size()
        self.assertEqual(out, mock_content_length)

    @patch('records_mover.url.s3.s3_file_url.S3Concat')
    def test_concatenate_from(self,
                              mock_S3Concat):
        mock_loc_1 = MagicMock(name='loc_1', spec=S3FileUrl)
        mock_loc_1.bucket = 'bucket'
        mock_loc_1.key = 'somekey1'
        mock_loc_2 = MagicMock(name='loc_2', spec=S3FileUrl)
        mock_loc_2.bucket = 'bucket'
        mock_loc_2.key = 'somekey2'
        mock_job = mock_S3Concat.return_value
        mock_output_file_details = Mock(name='output_file_details')
        mock_job.concat.return_value = [mock_output_file_details]
        self.s3_file_url.concatenate_from([mock_loc_1, mock_loc_2])
        mock_S3Concat.assert_called_with('bucket',
                                         'topdir/bottomdir/file',
                                         session=self.mock_boto3_session,
                                         min_file_size=None)
        mock_job.add_file.assert_any_call(mock_loc_1.key)
        mock_job.add_file.assert_any_call(mock_loc_2.key)
        mock_job.concat.assert_called_with()
