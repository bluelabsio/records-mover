import unittest
import records_mover


class TestTopLevel(unittest.TestCase):
    def test_sources(self):
        self.assertEqual(type(records_mover.sources),
                         records_mover.records.sources.factory.RecordsSources)

    def test_targets(self):
        self.assertEqual(type(records_mover.targets),
                         records_mover.records.targets.factory.RecordsTargets)
