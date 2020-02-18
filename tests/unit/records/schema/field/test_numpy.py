import unittest
from mock import Mock, patch
from records_mover.records.schema.field.numpy import details_from_numpy_dtype
import numpy as np


class TestNumpy(unittest.TestCase):
    @patch('records_mover.records.schema.field.numpy.RecordsSchemaFieldConstraints')
    def test_details_from_numpy_dtype(self,
                                      mock_RecordsSchemaFieldConstraints):
        tests = {
            np.dtype(str): 'string',
            np.dtype(int): 'integer',
            np.dtype(np.datetime64): 'datetime',
        }
        for dtype, expected_field_type in tests.items():
            mock_unique = Mock(name='unique')
            actual_field_type, actual_constraints = details_from_numpy_dtype(dtype=dtype,
                                                                             unique=mock_unique)
            self.assertEqual(actual_field_type, expected_field_type)
