import logging
import json
from .sqlalchemy import schema_from_db_table, schema_to_schema_sql
from typing import List, Dict, Mapping, IO, Any, TYPE_CHECKING
from ..field import RecordsSchemaField
from ...records_format import BaseRecordsFormat
from ...processing_instructions import ProcessingInstructions
from .known_representation import RecordsSchemaKnownRepresentation
from ..errors import UnsupportedSchemaError
if TYPE_CHECKING:
    from pandas import DataFrame

    from ....db import DBDriver  # noqa
    from typing_extensions import Literal

    from mypy_extensions import TypedDict

    from ..field import FieldDict
    from .known_representation import KnownRepresentationDict

    class MandatoryRecordsSchemaDict(TypedDict):
        schema: Literal['bltypes/v1']
        fields: Dict[str, FieldDict]

    class RecordsSchemaDict(MandatoryRecordsSchemaDict, total=False):
        known_representations: Dict[str, KnownRepresentationDict]


logger = logging.getLogger(__name__)


class RecordsSchema:
    def __init__(self,
                 fields: List[RecordsSchemaField],
                 known_representations: Mapping[str, RecordsSchemaKnownRepresentation]) -> None:
        self.fields = fields
        self.known_representations = known_representations

    def __str__(self) -> str:
        types = {field.name: field.field_type for field in self.fields}
        return f"RecordsSchema(types={types})"

    @staticmethod
    def from_db_table(schema_name: str, table_name: str, driver: 'DBDriver') -> 'RecordsSchema':
        return schema_from_db_table(schema_name=schema_name,
                                    table_name=table_name,
                                    driver=driver)

    def to_data(self) -> 'RecordsSchemaDict':
        data: 'RecordsSchemaDict' = {
            'schema': 'bltypes/v1',
            'fields': {},
        }
        i = 1
        for field in self.fields:
            data['fields'][field.name] = field.to_data()
            data['fields'][field.name]['index'] = i
            i = i + 1
        if self.known_representations:
            data['known_representations'] = {
                system: known_representation.to_data()
                for system, known_representation
                in self.known_representations.items()
            }
        return data

    def to_json(self) -> str:
        return json.dumps(self.to_data())

    @staticmethod
    def from_data(data: 'RecordsSchemaDict') -> 'RecordsSchema':
        schema_ver = data["schema"]
        if schema_ver != "bltypes/v1":
            raise UnsupportedSchemaError(
                f"unsupported record specification schema {schema_ver}")
        known_representations = {}
        for name, known_rep_data in data.get("known_representations", {}).items():
            known_representation = RecordsSchemaKnownRepresentation.from_data(known_rep_data)
            if known_representation is not None:
                known_representations[name] = known_representation
        return RecordsSchema(
            fields=[
                RecordsSchemaField.from_data(name, field_data)
                for name, field_data
                in data.get("fields", {}).items()
            ],
            known_representations=known_representations)

    @staticmethod
    def from_json(json_str: str) -> 'RecordsSchema':
        data = json.loads(json_str)
        return RecordsSchema.from_data(data)

    def to_schema_sql(self,
                      driver: 'DBDriver',
                      schema_name: str,
                      table_name: str) -> str:
        return schema_to_schema_sql(records_schema=self,
                                    driver=driver,
                                    schema_name=schema_name,
                                    table_name=table_name)

    @staticmethod
    def from_fileobjs(fileobjs: List[IO[bytes]],
                      records_format: BaseRecordsFormat,
                      processing_instructions: ProcessingInstructions) -> 'RecordsSchema':
        from records_mover.records.csv_streamer import stream_csv
        from records_mover.pandas import purge_unnamed_unused_columns

        if len(fileobjs) != 1:
            # https://app.asana.com/0/53283930106309/1131698268455054
            raise NotImplementedError('Cannot currently sniff schema from mulitple '
                                      'files--please provide explicit schema JSON')
        fileobj = fileobjs[0]
        if not fileobj.seekable():
            raise NotImplementedError('Cannot currently sniff schema from a pure stream--'
                                      'please save file to disk and load from there or '
                                      'provide explicit schema JSON')
        with stream_csv(fileobj, records_format.hints) as reader:  # type: ignore
            # Parse schema from sample df

            sample_row_count = processing_instructions.max_inference_rows
            if sample_row_count is not None:
                df = reader.get_chunk(sample_row_count)
            else:
                df = reader.read()

            fileobj.seek(0)

            df = purge_unnamed_unused_columns(df)
            schema = RecordsSchema.from_dataframe(df, processing_instructions,
                                                  include_index=False)

            schema.refine_from_dataframe(df,
                                         processing_instructions=processing_instructions)
            return schema

    def refine_from_dataframe(self,
                              df: 'DataFrame',
                              processing_instructions:
                              ProcessingInstructions = ProcessingInstructions()) -> None:
        """
        Adjust records schema based on facts found from a dataframe.
        """
        from .pandas import refine_schema_from_dataframe
        return refine_schema_from_dataframe(records_schema=self,
                                            df=df,
                                            processing_instructions=processing_instructions)

    def cast_dataframe_types(self,
                             df: 'DataFrame') -> 'DataFrame':
        """
        Returns a new dataframe with types that match what we know from this records schema.
        """
        col_mappings = {field.name: field.to_numpy_dtype() for field in self.fields}
        if len(col_mappings) == 0:
            # .as_type() doesn't like being given an empty map!
            return df
        return df.astype(col_mappings)

    def assign_dataframe_names(self,
                               include_index: bool,
                               df: 'DataFrame') -> 'DataFrame':
        """Returns a new dataframe with index/series names that match what we
        know from this records schema.  Useful when we've created a
        dataframe from a file where the headers don't match the exact
        names or when the names aren't provided
        """
        df_names = [label for label in df]
        field_names = [field.name for field in self.fields]
        index_mapping = None
        if include_index:
            index_name = field_names[0]
            # RecordsMover supports only single indexes for the moment.
            #
            # https://app.asana.com/0/1128138765527694/1161071033650873
            index_mapping = {0: index_name}

            # remove first name, which applies to index
            field_names = field_names[1:]
        if len(df_names) != len(field_names):
            raise SyntaxError("Number of DataFrame names didn't match number of schema names!"
                              f"DataFrame: {df_names}.  Schema names: {field_names}")
        name_mapping = dict(zip(df_names, field_names))
        df = df.rename(columns=name_mapping,
                       copy=False,
                       errors='raise')
        if include_index:
            return df.rename(index=index_mapping,
                             copy=False,
                             errors='raise')
        else:
            return df

    @staticmethod
    def from_dataframe(df: 'DataFrame',
                       processing_instructions: ProcessingInstructions,
                       include_index: bool) -> 'RecordsSchema':
        from .pandas import schema_from_dataframe
        return schema_from_dataframe(df=df,
                                     processing_instructions=processing_instructions,
                                     include_index=include_index)

    def to_empty_dataframe(self) -> 'DataFrame':
        from pandas import DataFrame
        df_data: Dict[str, List[Any]] = {field.name: [] for field in self.fields}
        raw_df = DataFrame(df_data)
        return self.cast_dataframe_types(raw_df)

    def convert_datetimes_to_datetimetz(self) -> 'RecordsSchema':
        return RecordsSchema(fields=[field.convert_datetime_to_datetimetz()
                                     for field in self.fields],
                             known_representations=self.known_representations)
