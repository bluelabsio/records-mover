import pandas as pd
import numpy as np
import unittest
from records_mover import pandas


class TestPandas(unittest.TestCase):
    def test_dataframe_to_nested_dicts(self):
        data = np.array([(1, 5, 'bazzle')],
                        dtype=[
                            ('abc', 'i4'),
                            ('foo.bar', 'i4'),
                            ('foo.baz.bing', 'U10')
                        ])

        mock_dataframe = pd.DataFrame(data)

        expected_dicts = [
            {'abc': 1, 'foo': {'bar': 5, 'baz': {'bing': 'bazzle'}}}
        ]

        self.assertEquals(pandas.dataframe_to_nested_dicts(mock_dataframe),
                          expected_dicts)

    def test_dataframe_to_nested_dicts_snake_to_camel(self):
        data = np.array([(1, 5, 'bazzle', 'whatevs')],
                        dtype=[
                            ('abc_def', 'i4'),
                            ('foo.bar', 'i4'),
                            ('foo.baz.bing_blong', 'U10'),
                            ('test_first.test_second.test_last_in_chain',
                             'U10'),
                        ])

        mock_dataframe = pd.DataFrame(data)

        expected_dicts = [
            {
                'abcDef': 1,
                'foo': {'bar': 5, 'baz': {'bingBlong': 'bazzle'}},
                'testFirst': {'testSecond': {'testLastInChain': 'whatevs'}},
            },
        ]

        self.assertEquals(pandas.dataframe_to_nested_dicts(mock_dataframe,
                                                           to_camel=True),
                          expected_dicts)

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
        self.assertEquals(pandas.json_dumps(data), expected_json)

    def test_json_dumps_object_not_handled(self):
        class MyNewClass:
            pass

        data = [MyNewClass()]

        with self.assertRaises(TypeError):
            pandas.json_dumps(data)
