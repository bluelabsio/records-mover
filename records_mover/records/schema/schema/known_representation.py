import logging
import json
from ...processing_instructions import ProcessingInstructions
from abc import ABCMeta, abstractmethod
from typing import Dict, Any, cast, Optional, TYPE_CHECKING
if TYPE_CHECKING:
    from ....db import DBDriver  # noqa
    from mypy_extensions import TypedDict
    from pandas import DataFrame

    class KnownRepresentationDict(TypedDict):
        type: str

    class SqlKnownRepresentationDict(KnownRepresentationDict, total=False):
        table_ddl: str

    class CorePandasDataframeKnownRepresentationDict(KnownRepresentationDict):
        pd_df_dtypes: Dict[str, Dict[str, Any]]

    class PandasDataframeKnownRepresentationDict(CorePandasDataframeKnownRepresentationDict,
                                                 total=False):
        pd_df_ftypes: Dict[str, str]


logger = logging.getLogger(__name__)


class RecordsSchemaKnownRepresentation(metaclass=ABCMeta):

    @staticmethod
    def from_data(data: 'KnownRepresentationDict') -> Optional['RecordsSchemaKnownRepresentation']:
        if data['type'].startswith('sql/'):
            sql_data = cast('SqlKnownRepresentationDict', data)
            return RecordsSchemaSqlKnownRepresentation(type=sql_data['type'],
                                                       table_ddl=sql_data.get('table_ddl'))
        elif data['type'].startswith('dataframe/pandas'):
            pandas_data = cast('PandasDataframeKnownRepresentationDict', data)
            return RecordsSchemaPandasDataframeKnownRepresentation(
                pd_df_dtypes=pandas_data['pd_df_dtypes'],
                pd_df_ftypes=pandas_data.get('pd_df_ftypes'))
        else:
            logger.warning(f"Unknown known-representation type ({data['type']})--"
                           "consider upgrading records-mover")
            return None

    @staticmethod
    def from_dataframe(df: 'DataFrame',
                       processing_instructions: ProcessingInstructions) ->\
            'RecordsSchemaKnownRepresentation':
        dtypes_json = df.dtypes.to_json()
        dtypes_data = json.loads(dtypes_json)
        ftypes_data = None
        if getattr(df, 'ftypes', None) is not None:
            ftypes_json = df.ftypes.to_json()
            ftypes_data = json.loads(ftypes_json)
        return RecordsSchemaPandasDataframeKnownRepresentation(pd_df_dtypes=dtypes_data,
                                                               pd_df_ftypes=ftypes_data)

    @staticmethod
    def from_db_driver(driver: 'DBDriver',
                       schema_name: str,
                       table_name: str) -> 'RecordsSchemaSqlKnownRepresentation':
        type = f"sql/{driver.db.dialect.name}"
        ddl = driver.schema_sql(schema_name, table_name)
        return RecordsSchemaSqlKnownRepresentation(type=type, table_ddl=ddl)

    @abstractmethod
    def to_data(self) -> 'KnownRepresentationDict':
        raise NotImplementedError


class RecordsSchemaSqlKnownRepresentation(RecordsSchemaKnownRepresentation):
    def __init__(self, type: str, table_ddl: Optional[str]) -> None:
        self.type = type
        self.table_ddl = table_ddl

    def to_data(self) -> 'SqlKnownRepresentationDict':
        out: 'SqlKnownRepresentationDict' = {
            'type': self.type
        }
        if self.table_ddl is not None:
            out['table_ddl'] = self.table_ddl
        return out


class RecordsSchemaPandasDataframeKnownRepresentation(RecordsSchemaKnownRepresentation):
    def __init__(self,
                 pd_df_dtypes: Dict[str, Dict[str, Any]],
                 pd_df_ftypes: Optional[Dict[str, str]] = None) -> None:
        self.pd_df_dtypes = pd_df_dtypes
        self.pd_df_ftypes = pd_df_ftypes

    def to_data(self) -> 'PandasDataframeKnownRepresentationDict':
        out: 'PandasDataframeKnownRepresentationDict' = {
            'type': 'dataframe/pandas',
            'pd_df_dtypes': self.pd_df_dtypes,
        }
        if self.pd_df_ftypes is not None:
            out['pd_df_ftypes'] = self.pd_df_ftypes
        return out
