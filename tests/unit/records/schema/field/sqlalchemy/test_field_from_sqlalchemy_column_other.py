from mock import Mock, patch
import sqlalchemy
from .base_test_field_from_sqlalchemy_column import BaseTestSqlAlchemyFieldFromSqlalchemyColumn


@patch('records_mover.records.schema.field.RecordsSchemaField')
@patch('records_mover.records.schema.field.sqlalchemy.RecordsSchemaFieldRepresentation')
class TestSqlAlchemyFieldFromSqlalchemyColumnOther(BaseTestSqlAlchemyFieldFromSqlalchemyColumn):
    @patch('records_mover.records.schema.field.sqlalchemy.RecordsSchemaFieldStringConstraints')
    def test_string(self,
                    mock_RecordsSchemaFieldStringConstraints,
                    mock_RecordsSchemaFieldRepresentation,
                    mock_RecordsSchemaField):
        mock_type_ = sqlalchemy.sql.sqltypes.String(123)
        self.verify(mock_RecordsSchemaFieldStringConstraints,
                    mock_RecordsSchemaFieldRepresentation,
                    mock_RecordsSchemaField,
                    field_type='string',
                    type_=mock_type_,
                    constraints=mock_RecordsSchemaFieldStringConstraints.
                    from_sqlalchemy_type.return_value)

    @patch('records_mover.records.schema.field.sqlalchemy.RecordsSchemaFieldConstraints')
    def test_date(self,
                  mock_RecordsSchemaFieldConstraints,
                  mock_RecordsSchemaFieldRepresentation,
                  mock_RecordsSchemaField):

        mock_type_ = sqlalchemy.sql.sqltypes.Date()
        self.verify(mock_RecordsSchemaFieldConstraints,
                    mock_RecordsSchemaFieldRepresentation,
                    mock_RecordsSchemaField,
                    field_type='date',
                    type_=mock_type_,
                    constraints=mock_RecordsSchemaFieldConstraints.
                    from_sqlalchemy_type.return_value)

    @patch('records_mover.records.schema.field.sqlalchemy.RecordsSchemaFieldConstraints')
    def test_datetime_simple_tz(self,
                                mock_RecordsSchemaFieldConstraints,
                                mock_RecordsSchemaFieldRepresentation,
                                mock_RecordsSchemaField):
        mock_OnlyOneType = Mock(name='OnlyOneType')

        def type_for_date_plus_time(has_tz):
            return mock_OnlyOneType.return_value

        self.mock_driver.type_for_date_plus_time = type_for_date_plus_time
        mock_type_ = sqlalchemy.sql.sqltypes.DateTime()
        mock_type_.timezone = True
        self.verify(mock_RecordsSchemaFieldConstraints,
                    mock_RecordsSchemaFieldRepresentation,
                    mock_RecordsSchemaField,
                    field_type='datetimetz',
                    type_=mock_type_,
                    constraints=mock_RecordsSchemaFieldConstraints.
                    from_sqlalchemy_type.return_value)

    @patch('records_mover.records.schema.field.sqlalchemy.RecordsSchemaFieldConstraints')
    def test_datetime_simple_no_tz(self,
                                   mock_RecordsSchemaFieldConstraints,
                                   mock_RecordsSchemaFieldRepresentation,
                                   mock_RecordsSchemaField):
        mock_OnlyOneType = Mock(name='OnlyOneType')

        def type_for_date_plus_time(has_tz):
            return mock_OnlyOneType.return_value

        self.mock_driver.type_for_date_plus_time = type_for_date_plus_time
        mock_type_ = sqlalchemy.sql.sqltypes.DateTime()
        mock_type_.timezone = False
        self.verify(mock_RecordsSchemaFieldConstraints,
                    mock_RecordsSchemaFieldRepresentation,
                    mock_RecordsSchemaField,
                    field_type='datetime',
                    type_=mock_type_,
                    constraints=mock_RecordsSchemaFieldConstraints.
                    from_sqlalchemy_type.return_value)

    @patch('records_mover.records.schema.field.sqlalchemy.RecordsSchemaFieldConstraints')
    def test_datetime_complex_tz(self,
                                 mock_RecordsSchemaFieldConstraints,
                                 mock_RecordsSchemaFieldRepresentation,
                                 mock_RecordsSchemaField):
        # Let's act like BigQuery:
        def type_for_date_plus_time(has_tz):
            if has_tz:
                return sqlalchemy.sql.sqltypes.TIMESTAMP()
            else:
                return sqlalchemy.sql.sqltypes.DATETIME()

        self.mock_driver.type_for_date_plus_time = type_for_date_plus_time
        mock_type_ = sqlalchemy.sql.sqltypes.TIMESTAMP()
        self.verify(mock_RecordsSchemaFieldConstraints,
                    mock_RecordsSchemaFieldRepresentation,
                    mock_RecordsSchemaField,
                    field_type='datetimetz',
                    type_=mock_type_,
                    constraints=mock_RecordsSchemaFieldConstraints.
                    from_sqlalchemy_type.return_value)

    @patch('records_mover.records.schema.field.sqlalchemy.RecordsSchemaFieldConstraints')
    def test_datetime_complex_no_tz(self,
                                    mock_RecordsSchemaFieldConstraints,
                                    mock_RecordsSchemaFieldRepresentation,
                                    mock_RecordsSchemaField):
        # Let's act like BigQuery:
        def type_for_date_plus_time(has_tz):
            if has_tz:
                return sqlalchemy.sql.sqltypes.TIMESTAMP()
            else:
                return sqlalchemy.sql.sqltypes.DATETIME()

        self.mock_driver.type_for_date_plus_time = type_for_date_plus_time
        mock_type_ = sqlalchemy.sql.sqltypes.DATETIME()
        self.verify(mock_RecordsSchemaFieldConstraints,
                    mock_RecordsSchemaFieldRepresentation,
                    mock_RecordsSchemaField,
                    field_type='datetime',
                    type_=mock_type_,
                    constraints=mock_RecordsSchemaFieldConstraints.
                    from_sqlalchemy_type.return_value)

    @patch('records_mover.records.schema.field.sqlalchemy.RecordsSchemaFieldConstraints')
    def test_time_no_tz(self,
                        mock_RecordsSchemaFieldConstraints,
                        mock_RecordsSchemaFieldRepresentation,
                        mock_RecordsSchemaField):
        mock_type_ = sqlalchemy.sql.sqltypes.Time()
        mock_type_.timezone = False
        self.verify(mock_RecordsSchemaFieldConstraints,
                    mock_RecordsSchemaFieldRepresentation,
                    mock_RecordsSchemaField,
                    field_type='time',
                    type_=mock_type_,
                    constraints=mock_RecordsSchemaFieldConstraints.
                    from_sqlalchemy_type.return_value)

    @patch('records_mover.records.schema.field.sqlalchemy.RecordsSchemaFieldConstraints')
    def test_time_tz(self,
                     mock_RecordsSchemaFieldConstraints,
                     mock_RecordsSchemaFieldRepresentation,
                     mock_RecordsSchemaField):
        mock_type_ = sqlalchemy.sql.sqltypes.Time()
        mock_type_.timezone = True
        self.verify(mock_RecordsSchemaFieldConstraints,
                    mock_RecordsSchemaFieldRepresentation,
                    mock_RecordsSchemaField,
                    field_type='timetz',
                    type_=mock_type_,
                    constraints=mock_RecordsSchemaFieldConstraints.
                    from_sqlalchemy_type.return_value)

    @patch('records_mover.records.schema.field.sqlalchemy.RecordsSchemaFieldConstraints')
    def test_boolean(self,
                     mock_RecordsSchemaFieldConstraints,
                     mock_RecordsSchemaFieldRepresentation,
                     mock_RecordsSchemaField):
        mock_type_ = sqlalchemy.sql.sqltypes.Boolean()
        self.verify(mock_RecordsSchemaFieldConstraints,
                    mock_RecordsSchemaFieldRepresentation,
                    mock_RecordsSchemaField,
                    field_type='boolean',
                    type_=mock_type_,
                    constraints=mock_RecordsSchemaFieldConstraints.
                    from_sqlalchemy_type.return_value)
