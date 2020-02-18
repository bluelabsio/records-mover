import unittest
from mock import Mock, patch
from records_mover.records.schema.field.representation import RecordsSchemaFieldRepresentation


class TestFieldRepresentation(unittest.TestCase):
    maxDiff = None

    @patch('records_mover.records.schema.field.representation.CreateColumn')
    def test_from_sqlalchemy_column_splitting(self,
                                              mock_CreateColumn):
        mock_column = Mock(name='column')
        mock_dialect = Mock(name='dialect')
        mock_rep_type = Mock(name='rep_type')

        test_cases = [
            ('BOOLEAN', 'BOOLEAN', None),
            ('REAL', 'REAL', None),
            ('NUMERIC(2, 1)', 'NUMERIC(2, 1)', None),
            ('INTEGER ENCODE lzo', 'INTEGER', 'ENCODE lzo'),
            ('NUMERIC(2, 1) ENCODE lzo', 'NUMERIC(2, 1)', 'ENCODE lzo'),
            ('SMALLINT ENCODE lzo', 'SMALLINT', 'ENCODE lzo'),
            ('DOUBLE PRECISION', 'DOUBLE PRECISION', None),
            ('DOUBLE PRECISION SORTKEY', 'DOUBLE PRECISION', 'SORTKEY'),
            ('TIMESTAMP WITH TIME ZONE ENCODE lzo', 'TIMESTAMP WITH TIME ZONE', 'ENCODE lzo'),
            ('TIMESTAMP WITHOUT TIME ZONE ENCODE lzo', 'TIMESTAMP WITHOUT TIME ZONE', 'ENCODE lzo'),
        ]
        for test_case in test_cases:
            expected_col_ddl, expected_col_type, expected_col_modifiers = test_case
            full_col_ddl = f'COLNAME {expected_col_ddl}'

            mock_CreateColumn.return_value.compile.return_value = full_col_ddl
            representation = RecordsSchemaFieldRepresentation.\
                from_sqlalchemy_column(mock_column,
                                       mock_dialect,
                                       rep_type=mock_rep_type)
            self.assertEqual(representation.col_ddl, expected_col_ddl)
            self.assertEqual(representation.col_type, expected_col_type)
            self.assertEqual(representation.col_modifiers, expected_col_modifiers)

    def test_from_data_future_rep_type(self):
        data = {
            'rep_type': 'future_thing_i/added'
        }
        out = RecordsSchemaFieldRepresentation.from_data(data)
        self.assertIsNone(out)
