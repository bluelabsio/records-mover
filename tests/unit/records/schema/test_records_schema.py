import unittest
from mock import Mock, patch, ANY
from pandas import DataFrame
from records_mover.records.schema import RecordsSchema


class TestRecordsSchema(unittest.TestCase):
    maxDiff = None

    @patch('records_mover.records.schema.schema.sqlalchemy.RecordsSchemaField')
    @patch('records_mover.records.schema.schema.sqlalchemy.RecordsSchemaKnownRepresentation')
    def test_from_db_table(self,
                           mock_RecordsSchemaKnownRepresentation,
                           mock_RecordsSchemaField):
        mock_schema_name = Mock(name='schema_name')
        mock_table_name = Mock(name='table_name')
        mock_driver = Mock(name='driver')
        mock_column = Mock(name='column')
        mock_table = mock_driver.table.return_value
        mock_table.columns = [mock_column]
        mock_origin_representation =\
            mock_RecordsSchemaKnownRepresentation.from_db_driver.return_value

        mock_known_representations = {
            'origin': mock_origin_representation,
        }
        mock_field = mock_RecordsSchemaField.from_sqlalchemy_column.return_value
        actual_schema = RecordsSchema.from_db_table(schema_name=mock_schema_name,
                                                    table_name=mock_table_name,
                                                    driver=mock_driver)
        mock_driver.table.assert_called_with(mock_schema_name, mock_table_name)
        mock_RecordsSchemaKnownRepresentation.from_db_driver.assert_called_with(mock_driver,
                                                                                mock_schema_name,
                                                                                mock_table_name)
        mock_RecordsSchemaField.\
            from_sqlalchemy_column.assert_called_with(column=mock_column,
                                                      driver=mock_driver,
                                                      rep_type=mock_origin_representation.type)

        self.assertEqual(actual_schema.fields, [mock_field])
        self.assertEqual(actual_schema.known_representations,
                         mock_known_representations)

    def test_str(self):
        obj = RecordsSchema(fields=[],
                            known_representations={})
        self.assertEqual(str(obj), "RecordsSchema(types={})")

    @patch('records_mover.records.schema.schema.schema_to_schema_sql')
    def test_to_schema_sql(self, mock_schema_to_schema_sql):
        mock_driver = Mock(name='driver')
        mock_schema_name = Mock(name='schema_name')
        mock_table_name = Mock(name='table_name')
        obj = RecordsSchema(fields=[],
                            known_representations={})
        out = obj.to_schema_sql(mock_driver,
                                mock_schema_name,
                                mock_table_name)
        mock_schema_to_schema_sql.assert_called_with(driver=mock_driver,
                                                     records_schema=obj,
                                                     schema_name=mock_schema_name,
                                                     table_name=mock_table_name)
        self.assertEqual(out, mock_schema_to_schema_sql.return_value)

    @patch('records_mover.records.schema.schema.RecordsSchema')
    @patch('records_mover.records.schema.schema.stream_csv')
    def test_from_fileobjs(self,
                           mock_stream_csv,
                           mock_RecordsSchema):
        mock_fileobj = Mock(name='fileobj')
        mock_fileobjs = [mock_fileobj]
        mock_records_format = Mock(name='records_format')
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_fileobj.seekable.return_value = True
        mock_reader = mock_stream_csv.return_value.__enter__.return_value
        data = [
            {'Country': 'Belgium', 'Capital': 'Brussels', 'Population': 11190846,
             'Unnamed: 1': None},
            {'Country': 'India', 'Capital': 'New Delhi', 'Population': 1303171035,
             'Unnamed: 1': None},
            {'Country': 'Brazil', 'Capital': 'Brasília', 'Population': 207847528,
             'Unnamed: 1': None},
        ]
        df = DataFrame.from_dict(data)
        mock_reader.get_chunk.return_value = df
        out = RecordsSchema.from_fileobjs(mock_fileobjs,
                                          mock_records_format,
                                          mock_processing_instructions)
        mock_reader.get_chunk.assert_called_with(mock_processing_instructions.max_inference_rows)
        mock_fileobj.seek.assert_called_with(0)
        mock_RecordsSchema.from_dataframe.assert_called_with(ANY,
                                                             mock_processing_instructions,
                                                             include_index=False)
        actual_cleaned_up_df = mock_RecordsSchema.from_dataframe.mock_calls[0][1][0]
        actual_cleaned_up_df_data = actual_cleaned_up_df.to_dict(orient='records')
        expected_cleaned_up_df_data = [
            {'Country': 'Belgium', 'Capital': 'Brussels', 'Population': 11190846},
            {'Country': 'India', 'Capital': 'New Delhi', 'Population': 1303171035},
            {'Country': 'Brazil', 'Capital': 'Brasília', 'Population': 207847528},
        ]
        self.assertEqual(actual_cleaned_up_df_data, expected_cleaned_up_df_data)
        self.assertEqual(out, mock_RecordsSchema.from_dataframe.return_value)

    @patch('records_mover.records.schema.schema.RecordsSchema')
    @patch('records_mover.records.schema.schema.stream_csv')
    def test_from_fileobjs_no_max_inference_rows(self,
                                                 mock_stream_csv,
                                                 mock_RecordsSchema):
        mock_fileobj = Mock(name='fileobj')
        mock_fileobjs = [mock_fileobj]
        mock_records_format = Mock(name='records_format')
        mock_processing_instructions = Mock(name='processing_instructions')
        mock_processing_instructions.max_inference_rows = None
        mock_fileobj.seekable.return_value = True
        mock_reader = mock_stream_csv.return_value.__enter__.return_value
        data = [
            {'Country': 'Belgium', 'Capital': 'Brussels', 'Population': 11190846,
             'Unnamed: 1': None},
            {'Country': 'India', 'Capital': 'New Delhi', 'Population': 1303171035,
             'Unnamed: 1': None},
            {'Country': 'Brazil', 'Capital': 'Brasília', 'Population': 207847528,
             'Unnamed: 1': None},
        ]
        df = DataFrame.from_dict(data)
        mock_reader.read.return_value = df
        out = RecordsSchema.from_fileobjs(mock_fileobjs,
                                          mock_records_format,
                                          mock_processing_instructions)
        mock_fileobj.seek.assert_called_with(0)
        mock_RecordsSchema.from_dataframe.assert_called_with(ANY,
                                                             mock_processing_instructions,
                                                             include_index=False)
        actual_cleaned_up_df = mock_RecordsSchema.from_dataframe.mock_calls[0][1][0]
        actual_cleaned_up_df_data = actual_cleaned_up_df.to_dict(orient='records')
        expected_cleaned_up_df_data = [
            {'Country': 'Belgium', 'Capital': 'Brussels', 'Population': 11190846},
            {'Country': 'India', 'Capital': 'New Delhi', 'Population': 1303171035},
            {'Country': 'Brazil', 'Capital': 'Brasília', 'Population': 207847528},
        ]
        self.assertEqual(actual_cleaned_up_df_data, expected_cleaned_up_df_data)
        self.assertEqual(out, mock_RecordsSchema.from_dataframe.return_value)

    @patch('records_mover.records.schema.schema.refine_schema_from_dataframe')
    def test_refine_from_dataframe(self,
                                   mock_refine_schema_from_dataframe):
        mock_fields = Mock(name='fields')
        mock_known_representations = Mock(name='known_representations')
        schema = RecordsSchema(fields=mock_fields,
                               known_representations=mock_known_representations)

        mock_df = Mock(name='df')
        mock_processing_instructions = Mock(name='processing_instructions')
        out = schema.refine_from_dataframe(mock_df, mock_processing_instructions)
        mock_refine_schema_from_dataframe.\
            assert_called_with(records_schema=schema,
                               df=mock_df,
                               processing_instructions=mock_processing_instructions)
        self.assertEqual(out, mock_refine_schema_from_dataframe.return_value)

    def test_cast_dataframe_types(self):
        mock_field_a = Mock(name='field_a')
        mock_fields = [mock_field_a]
        mock_known_representations = Mock(name='known_representations')
        schema = RecordsSchema(fields=mock_fields,
                               known_representations=mock_known_representations)
        mock_df = Mock(name='df')
        mock_col_mappings = {mock_field_a.name: mock_field_a.to_numpy_dtype.return_value}
        out = schema.cast_dataframe_types(mock_df)
        mock_df.astype.assert_called_with(mock_col_mappings)
        self.assertEqual(out, mock_df.astype.return_value)

    def test_cast_dataframe_types_no_fields(self):
        mock_fields = []
        mock_known_representations = Mock(name='known_representations')
        schema = RecordsSchema(fields=mock_fields,
                               known_representations=mock_known_representations)
        mock_df = Mock(name='df')
        out = schema.cast_dataframe_types(mock_df)
        self.assertEqual(out, mock_df)

    def test_assign_dataframe_names_no_index(self):
        data = [{'a': 1}]
        df = DataFrame.from_dict(data)
        mock_field_a = Mock(name='field_a')
        mock_field_a.name = 'mya'
        mock_fields = [mock_field_a]
        mock_known_representations = Mock(name='known_representations')
        schema = RecordsSchema(fields=mock_fields,
                               known_representations=mock_known_representations)
        out = schema.assign_dataframe_names(False, df)
        self.assertEqual(out.to_dict(orient='records'), [{'mya': 1}])

    def test_assign_dataframe_names_with_index(self):
        data = [{'b': 1}]
        df = DataFrame.from_dict(data)
        self.assertEqual(df.to_dict(orient='index'), {0: {'b': 1}})
        mock_field_a = Mock(name='field_a')
        mock_field_a.name = 'mya'
        mock_field_b = Mock(name='field_b')
        mock_field_b.name = 'myb'
        mock_fields = [mock_field_a, mock_field_b]
        mock_known_representations = Mock(name='known_representations')
        schema = RecordsSchema(fields=mock_fields,
                               known_representations=mock_known_representations)
        out = schema.assign_dataframe_names(True, df)
        self.assertEqual(out.to_dict(orient='records'), [{'myb': 1}])
        self.assertEqual(out.to_dict(orient='index'), {'mya': {'myb': 1}})
