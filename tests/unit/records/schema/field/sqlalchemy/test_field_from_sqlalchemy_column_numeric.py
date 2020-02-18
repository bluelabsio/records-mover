from mock import patch
import sqlalchemy
from .base_test_field_from_sqlalchemy_column import BaseTestSqlAlchemyFieldFromSqlalchemyColumn


@patch('records_mover.records.schema.field.RecordsSchemaField')
@patch('records_mover.records.schema.field.sqlalchemy.RecordsSchemaFieldRepresentation')
class TestSqlAlchemyFieldFromSqlalchemyColumnNumeric(BaseTestSqlAlchemyFieldFromSqlalchemyColumn):
    @patch('records_mover.records.schema.field.sqlalchemy.RecordsSchemaFieldIntegerConstraints')
    def test_integer(self,
                     mock_RecordsSchemaFieldIntegerConstraints,
                     mock_RecordsSchemaFieldRepresentation,
                     mock_RecordsSchemaField):
        mock_type_ = sqlalchemy.sql.sqltypes.Integer()
        self.verify(mock_RecordsSchemaFieldIntegerConstraints,
                    mock_RecordsSchemaFieldRepresentation,
                    mock_RecordsSchemaField,
                    field_type='integer',
                    type_=mock_type_,
                    constraints=mock_RecordsSchemaFieldIntegerConstraints.
                    from_sqlalchemy_type.return_value)

    @patch('records_mover.records.schema.field.sqlalchemy.RecordsSchemaFieldDecimalConstraints')
    def test_numeric(self,
                     mock_RecordsSchemaFieldDecimalConstraints,
                     mock_RecordsSchemaFieldRepresentation,
                     mock_RecordsSchemaField):
        mock_type_ = sqlalchemy.sql.sqltypes.Numeric(123, 456)
        self.verify(mock_RecordsSchemaFieldDecimalConstraints,
                    mock_RecordsSchemaFieldRepresentation,
                    mock_RecordsSchemaField,
                    field_type='decimal',
                    type_=mock_type_,
                    constraints=mock_RecordsSchemaFieldDecimalConstraints.
                    from_sqlalchemy_type.return_value)
