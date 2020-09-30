import unittest
import datetime
import pytz
from records_mover.records.schema.field.constraints import (
    RecordsSchemaFieldConstraints,
    RecordsSchemaFieldStringConstraints,
    RecordsSchemaFieldIntegerConstraints,
    RecordsSchemaFieldDecimalConstraints,
)
from records_mover.records.schema.field.statistics import (
    RecordsSchemaFieldStringStatistics,
)
from records_mover.records.schema.field.representation import RecordsSchemaPandasFieldRepresentation
from records_mover.records.schema.field import RecordsSchemaField
from records_mover.records.schema.field.pandas import refine_field_from_series
from records_mover.records.schema.field.field_types import RECORDS_FIELD_TYPES
import pandas as pd


class TestPandas(unittest.TestCase):
    def test_refine_field_from_series_more_specific(self) -> None:
        # This test is designed to break when a new field type is
        # introduced, so you can add new expectations and make sure
        # the code handles the new type!

        fields = {
            'integer': {
                'series': pd.Series([30, 35, 40]),
                'constraints_type': RecordsSchemaFieldIntegerConstraints,
                'statistics_type': type(None),
            },
            'decimal': {
                'series': pd.Series([30.0, 35.1, 40.2]),
                'constraints_type': RecordsSchemaFieldDecimalConstraints,
                'statistics_type': type(None),
            },
            'string': {
                'series': pd.Series(['a', 'b', 'c']),
                'constraints_type': RecordsSchemaFieldStringConstraints,
                'statistics_type': RecordsSchemaFieldStringStatistics,
            },
            'boolean': {
                'series': pd.Series([True, True, False]),
                'constraints_type': RecordsSchemaFieldConstraints,
                'statistics_type': type(None),
            },
            'date': {
                'series': pd.Series([datetime.date(2020, 1, 1)]),
                'constraints_type': RecordsSchemaFieldConstraints,
                'statistics_type': type(None),
            },
            'time': {
                'series': pd.Series([datetime.time(hour=12, minute=0, second=0)]),
                'constraints_type': RecordsSchemaFieldConstraints,
                'statistics_type': type(None),
            },
            'timetz': {
                'series': pd.Series([datetime.time(hour=12, minute=0, second=0,
                                                   tzinfo=pytz.timezone('US/Eastern'))]),
                # refine_field_from_series() is not smart enough to
                # distinguish whether the time objects inside it all
                # have timezones or not.
                'expected_field_type': 'time',
                'constraints_type': RecordsSchemaFieldConstraints,
                'statistics_type': type(None),
            },
            'datetime': {
                'series': pd.Series([datetime.datetime(2020, 1, 1, hour=12)]),
                'constraints_type': RecordsSchemaFieldConstraints,
                'statistics_type': type(None),
            },
            'datetimetz': {
                'series': pd.Series([datetime.datetime(2020, 1, 1, hour=12,
                                                       tzinfo=pytz.timezone('US/Eastern'))]),
                # refine_field_from_series() is not smart enough to
                # distinguish whether the datetime objects inside it all
                # have timezones or not.
                'expected_field_type': 'datetime',
                'constraints_type': RecordsSchemaFieldConstraints,
                'statistics_type': type(None),
            }
        }
        for field_type in RECORDS_FIELD_TYPES:
            constraints = RecordsSchemaFieldStringConstraints(required=True,
                                                              unique=False,
                                                              max_length_bytes=255,
                                                              max_length_chars=255)
            pandas_representation = RecordsSchemaPandasFieldRepresentation(pd_df_dtype={},
                                                                           pd_df_ftype=None,
                                                                           pd_df_coltype='series')
            field = RecordsSchemaField(name='testfield',
                                       field_type='string',
                                       constraints=constraints,
                                       statistics=None,
                                       representations={'pandas': pandas_representation})
            series = fields[field_type]['series']
            returned_field = refine_field_from_series(field,
                                                      series,
                                                      total_rows=10,
                                                      rows_sampled=10)
            if 'expected_field_type' in fields[field_type]:
                self.assertEquals(returned_field.field_type,
                                  fields[field_type]['expected_field_type'])
            else:
                self.assertEquals(returned_field.field_type, field_type)
            self.assertEquals(type(returned_field.constraints),
                              fields[field_type]['constraints_type'])
            self.assertEquals(type(returned_field.statistics),
                              fields[field_type]['statistics_type'])
