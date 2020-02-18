from records_mover.utils import idempotent_path
from records_mover.utils import filenames_from_local_directory
import unittest
import os
import shutil


class TestJobContextJSONSchema(unittest.TestCase):
    def setUp(self):
        self.test_path = 'some_test_path'

    def tearDown(self):
        shutil.rmtree(self.test_path)

    def test_idempotent_path(self):
        self.assertFalse(os.path.exists(self.test_path))
        idempotent_path(self.test_path)
        self.assertTrue(os.path.exists(self.test_path))
        idempotent_path(self.test_path)
        self.assertTrue(os.path.exists(self.test_path))

    def test_filenames_from_local_directory(self):
        idempotent_path(self.test_path)
        self.assertEqual(filenames_from_local_directory(self.test_path), [])
        with open(os.path.join(self.test_path, 'some_file.file'), 'w'):
            pass

        self.assertEqual(filenames_from_local_directory(self.test_path), ['some_file.file'])
