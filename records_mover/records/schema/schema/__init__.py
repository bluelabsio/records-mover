import logging
import json
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
    from ..field.field_types import FieldType  # noqa
    from .known_representation import KnownRepresentationDict

    class MandatoryRecordsSchemaDict(TypedDict):
        schema: Literal['bltypes/v1']
        fields: Dict[str, FieldDict]

    class RecordsSchemaDict(MandatoryRecordsSchemaDict, total=False):
        known_representations: Dict[str, KnownRepresentationDict]


logger = logging.getLogger(__name__)


class RecordsSchema:
    """This class records whatever type information we have available at
    the time of capture of records data so that future readers of the
    data can do a minimum of type inference on the remaining data for
    whatever the target system is.

    See the `Records Schema spec
    <https://github.com/bluelabsio/records-mover/blob/master/docs/schema/SCHEMAv1.md>`_
    for more details.
    """

    def __init__(self,
                 fields: List[RecordsSchemaField],
                 known_representations: Mapping[str, RecordsSchemaKnownRepresentation]) -> None:
        """
        :param fields: Ordered list of which fields are included in this schema.
        :param known_representations: Detailed information about how each field is intended to be
           represented in certain systems.  The key is the name of the known representation type
           (e.g., "origin" or some other short nickname for the type if it is not the origin - e.g.,
           "redshift"). The value is an object with a type field containing the type of source
           system (e.g., sql/redshift) and other, type-specific fields that may be useful for
           reconstructing the schema in the target system.
        """
        self.fields = fields
        self.known_representations = known_representations

    def __str__(self) -> str:
        types = {field.name: field.field_type for field in self.fields}
        return f"RecordsSchema(types={types})"

    @staticmethod
    def from_db_table(schema_name: str, table_name: str, driver: 'DBDriver') -> 'RecordsSchema':
        from .sqlalchemy import schema_from_db_table
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
        """Create a RecordsSchema object from a Python dictionary serialized form.

        :param data: Python dictionary containing the serialized data described in the `Records
           Schema spec
           <https://github.com/bluelabsio/records-mover/blob/master/docs/schema/SCHEMAv1.md>`_
        :return: RecordsSchema object suitable for passing to Records Mover source/target factory
           methods.
        """
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
        from .sqlalchemy import schema_to_schema_sql
        return schema_to_schema_sql(records_schema=self,
                                    driver=driver,
                                    schema_name=schema_name,
                                    table_name=table_name)

    @staticmethod
    def from_fileobjs(fileobjs: List[IO[bytes]],
                      records_format: BaseRecordsFormat,
                      processing_instructions: ProcessingInstructions) -> 'RecordsSchema':
        """
        Sniffs
        """
        from records_mover.records.delimited import stream_csv
        from records_mover.pandas import purge_unnamed_unused_columns

        if len(fileobjs) != 1:
            # https://github.com/bluelabsio/records-mover/issues/84
            raise NotImplementedError('Cannot currently sniff schema from multiple '
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

            schema = schema.refine_from_dataframe(df,
                                                  processing_instructions=processing_instructions)
            return schema

    def refine_from_dataframe(self,
                              df: 'DataFrame',
                              processing_instructions:
                              ProcessingInstructions = ProcessingInstructions()) -> 'RecordsSchema':
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
        fields = {field.name: field for field in self.fields}
        return df.apply(lambda series: fields[series.name].cast_series_type(series))

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
            # https://github.com/bluelabsio/records-mover/issues/92
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
        """Create a RecordsSchema object representing a Pandas dataframe.

        :param df: Pandas dataframe that should be analyzed to determine schema information.
        :param processing_instructions: Instructions used during creation of the records schema,
           including how much data to analyze to infer this schema.  This is of type
           :class:`records_mover.records.ProcessingInstructions`
        :param include_index: If true, the Pandas dataframe index column will be included in the
           move.

        :return: RecordsSchema object suitable for passing to Records Mover source/target factory
           methods.
        """
        from .pandas import schema_from_dataframe
        return schema_from_dataframe(df=df,
                                     processing_instructions=processing_instructions,
                                     include_index=include_index)

    def to_empty_dataframe(self) -> 'DataFrame':
        from pandas import DataFrame
        df_data: Dict[str, List[Any]] = {field.name: [] for field in self.fields}
        raw_df = DataFrame(df_data)
        return self.cast_dataframe_types(raw_df)

    def cast_field_types(self, typecasts: Dict['FieldType', 'FieldType']) -> 'RecordsSchema':
        return RecordsSchema(fields=[field.cast(typecasts.get(field.field_type,
                                                              field.field_type))
                                     for field in self.fields],
                             known_representations=self.known_representations)
