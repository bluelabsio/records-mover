import unittest
from mock import Mock, patch
from records_mover.records.schema.field.sqlalchemy import field_from_sqlalchemy_column


@patch('records_mover.records.schema.field.RecordsSchemaField')
@patch('records_mover.records.schema.field.sqlalchemy.RecordsSchemaFieldRepresentation')
class BaseTestSqlAlchemyFieldFromSqlalchemyColumn(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.mock_column = Mock(name='column')
        self.mock_driver = Mock(name='driver')
        self.mock_rep_type = Mock(name='rep_type')

        self.mock_name = self.mock_column.name
        self.mock_required = False
        self.mock_unique = None

    def verify(self,
               mock_RecordsSchemaFieldConstraints,
               mock_RecordsSchemaFieldRepresentation,
               mock_RecordsSchemaField,
               field_type,
               type_,
               constraints):
        self.mock_column.type = type_
        mock_statistics = None
        mock_representations = {
            'origin': mock_RecordsSchemaFieldRepresentation.from_sqlalchemy_column.return_value
        }
        out = field_from_sqlalchemy_column(self.mock_column, self.mock_driver, self.mock_rep_type)
        mock_RecordsSchemaFieldRepresentation.from_sqlalchemy_column.\
            assert_called_with(self.mock_column,
                               self.mock_driver.db.dialect,
                               self.mock_rep_type)
        expected = mock_RecordsSchemaField.return_value
        self.mock_field_type = field_type
        self.assertEqual(out, expected)
        mock_RecordsSchemaField.assert_called_with(name=self.mock_name,
                                                   field_type=self.mock_field_type,
                                                   constraints=constraints,
                                                   statistics=mock_statistics,
                                                   representations=mock_representations)
        mock_RecordsSchemaFieldConstraints.from_sqlalchemy_type.\
            assert_called_with(required=self.mock_required,
                               unique=self.mock_unique,
                               type_=type_,
                               driver=self.mock_driver)
