import pandas as pd
import numpy as np
import unittest
from records_mover import pandas


class TestPandas(unittest.TestCase):
    def test_json_dumps(self):
        data = [
            'a',
            1,
            1.5,
            np.int8(1),
            np.float16(1.5),
            np.array([1, 2, 3]),
            {}
        ]
        expected_json = '["a", 1, 1.5, 1, 1.5, [1, 2, 3], {}]'
        self.assertEqual(pandas.json_dumps(data), expected_json)

    def test_json_dumps_object_not_handled(self):
        class MyNewClass:
            pass

        data = [MyNewClass()]

        with self.assertRaises(TypeError):
            pandas.json_dumps(data)
