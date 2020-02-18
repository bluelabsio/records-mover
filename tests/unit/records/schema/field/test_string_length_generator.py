import unittest
from mock import Mock
from records_mover.records.schema.field.string_length_generator import generate_string_length


class TestStringLengthGenerator(unittest.TestCase):
    maxDiff = None

    def test_generate_string_length_with_both(self):
        test_cases = [
            {
                'driver': True,
                'driver_varchar_length_is_in_chars': True,
                'constraints_max_length_bytes': 123,
                'constraints_max_length_chars': None,
                'statistics_max_length_bytes': None,
                'statistics_max_length_chars': None,
                'expected_string_length': 123
            },
            {
                'driver': True,
                'driver_varchar_length_is_in_chars': True,
                'constraints_max_length_bytes': None,
                'constraints_max_length_chars': 123,
                'statistics_max_length_bytes': None,
                'statistics_max_length_chars': None,
                'expected_string_length': 123
            },
            {
                'driver': True,
                'driver_varchar_length_is_in_chars': False,
                'constraints_max_length_bytes': 123,
                'constraints_max_length_chars': None,
                'statistics_max_length_bytes': None,
                'statistics_max_length_chars': None,
                'expected_string_length': 123
            },
            {
                'driver': True,
                'driver_varchar_length_is_in_chars': False,
                'constraints_max_length_bytes': None,
                'constraints_max_length_chars': 123,
                'statistics_max_length_bytes': None,
                'statistics_max_length_chars': None,
                'expected_string_length': 492
            },
            {
                'driver': False,
                'constraints_max_length_bytes': None,
                'constraints_max_length_chars': None,
                'statistics_max_length_bytes': None,
                'statistics_max_length_chars': None,
                'expected_string_length': 256
            },
            {
                'driver': True,
                'driver_varchar_length_is_in_chars': True,
                'constraints_max_length_bytes': None,
                'constraints_max_length_chars': None,
                'statistics_max_length_bytes': 123,
                'statistics_max_length_chars': None,
                'expected_string_length': 123
            },
            {
                'driver': True,
                'driver_varchar_length_is_in_chars': True,
                'constraints_max_length_bytes': None,
                'constraints_max_length_chars': None,
                'statistics_max_length_bytes': None,
                'statistics_max_length_chars': 123,
                'expected_string_length': 123
            },
            {
                'driver': True,
                'driver_varchar_length_is_in_chars': False,
                'constraints_max_length_bytes': None,
                'constraints_max_length_chars': None,
                'statistics_max_length_bytes': 123,
                'statistics_max_length_chars': None,
                'expected_string_length': 123
            },
            {
                'driver': True,
                'driver_varchar_length_is_in_chars': False,
                'constraints_max_length_bytes': None,
                'constraints_max_length_chars': None,
                'statistics_max_length_bytes': None,
                'statistics_max_length_chars': 123,
                'expected_string_length': 492
            },
            {
                'driver': False,
                'constraints_max_length_bytes': None,
                'constraints_max_length_chars': None,
                'statistics_max_length_bytes': None,
                'statistics_max_length_chars': None,
                'expected_string_length': 256
            },
            {
                'driver': False,
                'constraints_max_length_bytes': 123,
                'constraints_max_length_chars': None,
                'statistics_max_length_bytes': 22,
                'statistics_max_length_chars': None,
                'expected_string_length': 123
            },
        ]
        for tc in test_cases:
            if tc['driver']:
                mock_driver = Mock(name='driver')
                mock_driver.varchar_length_is_in_chars.return_value =\
                    tc['driver_varchar_length_is_in_chars']
            else:
                mock_driver = None
            mock_constraints = Mock(name='constraints')
            mock_statistics = Mock(name='statistics')
            mock_constraints.max_length_bytes = tc['constraints_max_length_bytes']
            mock_constraints.max_length_chars = tc['constraints_max_length_chars']
            mock_statistics.max_length_bytes = tc['statistics_max_length_bytes']
            mock_statistics.max_length_chars = tc['statistics_max_length_chars']
            out = generate_string_length(mock_constraints, mock_statistics, mock_driver)
            self.assertEqual(out, tc['expected_string_length'])
