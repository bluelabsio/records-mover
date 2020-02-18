import unittest
from records_mover.records.targets.base import RecordsTarget


class TestBase(unittest.TestCase):
    def test_str(self):
        target = RecordsTarget()
        self.assertEqual(str(target), 'RecordsTarget')
