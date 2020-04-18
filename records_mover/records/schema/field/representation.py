from abc import ABCMeta, abstractmethod
import json
from collections import OrderedDict
import logging
import re
from typing import Optional, Dict, Union, Any, cast, TYPE_CHECKING
if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import Dialect
    from sqlalchemy import Column
    from typing_extensions import Literal
    from mypy_extensions import TypedDict
    import pandas

    class FieldRepresentationDict(TypedDict):
        rep_type: str

    class MandatorySqlFieldRepresentationDict(FieldRepresentationDict):
        col_ddl: str

    class SqlFieldRepresentationDict(MandatorySqlFieldRepresentationDict, total=False):
        col_type: str
        col_modifiers: str

    class CorePandasFieldRepresentationDict(FieldRepresentationDict):
        pd_df_dtype: Dict[str, Any]
        pd_df_coltype: Union[Literal['index'], Literal['series']]

    class PandasFieldRepresentationDict(CorePandasFieldRepresentationDict, total=False):
        pd_df_ftype: str


logger = logging.getLogger(__name__)


class RecordsSchemaFieldRepresentation(metaclass=ABCMeta):
    @staticmethod
    def from_data(data: 'FieldRepresentationDict') -> Optional['RecordsSchemaFieldRepresentation']:
        if data['rep_type'].startswith('sql/'):
            sql_data = cast('SqlFieldRepresentationDict', data)
            return RecordsSchemaSqlFieldRepresentation(col_ddl=sql_data['col_ddl'],
                                                       rep_type=sql_data['rep_type'],
                                                       col_type=sql_data.get('col_type'),
                                                       col_modifiers=sql_data.get('col_modifiers'))
        elif data['rep_type'] == 'dataframe/pandas':
            pandas_data = cast('PandasFieldRepresentationDict', data)
            return RecordsSchemaPandasFieldRepresentation(
                pd_df_dtype=pandas_data['pd_df_dtype'],
                pd_df_ftype=pandas_data.get('pd_df_ftype'),
                pd_df_coltype=pandas_data['pd_df_coltype'])
        else:
            logger.warning(f"I don't understand type {data['rep_type']}--"
                           "consider upgrading records-mover")
            return None

    @staticmethod
    def from_series(series: 'pandas.Series') -> 'RecordsSchemaFieldRepresentation':
        import pandas

        dtype_json_str = pandas.io.json.dumps(series.dtype)
        dtype_numpy_rep = json.loads(dtype_json_str)
        ftype = getattr(series, 'ftype', None)
        return RecordsSchemaPandasFieldRepresentation(pd_df_coltype='series',
                                                      pd_df_dtype=dtype_numpy_rep,
                                                      pd_df_ftype=ftype)

    @staticmethod
    def from_index(index: 'pandas.Index') -> 'RecordsSchemaFieldRepresentation':
        dtype_json_str = pandas.io.json.dumps(index.dtype)
        dtype_numpy_rep = json.loads(dtype_json_str)
        return RecordsSchemaPandasFieldRepresentation(pd_df_coltype='index',
                                                      pd_df_dtype=dtype_numpy_rep,
                                                      pd_df_ftype=index.ftype)

    @staticmethod
    def from_sqlalchemy_column(column: 'Column', dialect: 'Dialect',
                               rep_type: str) ->\
            'RecordsSchemaFieldRepresentation':
        from sqlalchemy.schema import CreateColumn

        statement = CreateColumn(column)
        col_ddl_compiler = statement.compile(dialect=dialect)
        full_col_ddl = str(col_ddl_compiler)

        #
        # Parse single string output from SQLAlchemy into what records
        # schema wants.  Sadly there's no universal way to get this
        # out of the SQLAlchemy drivers themselves - example code from
        # the Redshift driver:
        # https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/blob/6885b059c2d23423ae763678a46bbcdd1b3db433/sqlalchemy_redshift/dialect.py#L345
        #
        # These regexps are in order from most specfic to least so
        # that we can match early on the special cases without them
        # being caught by the more general ones.  If this code is not
        # working right for your case, please see the unit test in
        # test_field_representation and enhance it first to cover your
        # newly discovered oddity.
        #

        regexps: Dict[str, str] = OrderedDict()
        # First, cover cases where the types themselves have spaces in them:
        # e.g., DOUBLE PRECISION
        regexps['two_word_type_1'] = r'^([\S]+) (DOUBLE PRECISION)$'
        # e.g,, DOUBLE PRECISION SORTKEY
        regexps['two_word_type_1_with_col_modifiers'] = r'^([\S]+) (DOUBLE PRECISION) (.*)$'
        # e.g., TIMESTAMP WITH TIME ZONE
        regexps['two_word_type_2'] = r'^([\S]+) (TIMESTAMP WITH TIME ZONE)$'
        # e.g., TIMESTAMP WITH TIME ZONE ENCODE lzo
        regexps['two_word_type_2_with_col_modifiers'] = r'^([\S]+) (TIMESTAMP WITH TIME ZONE) (.*)$'
        # e.g., TIMESTAMP WITHOUT TIME ZONE
        regexps['two_word_type_3'] = r'^([\S]+) (TIMESTAMP WITHOUT TIME ZONE)$'
        # e.g., TIMESTAMP WITH TIME ZONE ENCODE lzo
        regexps['two_word_type_3_with_col_modifiers'] =\
            r'^([\S]+) (TIMESTAMP WITHOUT TIME ZONE) (.*)$'
        # e.g., NUMERIC(2, 1)
        regexps['parenthetical_col_type'] = r'^([\S]+) ([\S]+\(.*\))$'
        # e.g., NUMERIC(2, 1) ENCODE lzo
        regexps['parenthetical_col_type_with_col_modifiers'] = r'^([\S]+) ([\S]+\(.*\)) (.*)$'
        # e.g., REAL
        regexps['boring_col_type'] = r'^([\S]+) ([\S]+)$'
        # e.g., REAL ENCODE lzo
        regexps['boring_col_type_with_col_modifiers'] = r'^([\S]+) ([\S]+) (.*)$'

        for regexp in regexps.values():
            match = re.match(regexp, full_col_ddl)
            if match is not None:
                statement_words = list(match.groups())
                break
        else:
            raise SyntaxError(f"Could not understand {full_col_ddl}")
        col_ddl = ' '.join(statement_words[1:])
        col_type = statement_words[1]
        col_modifiers: Optional[str] = ' '.join(statement_words[2:])
        if col_modifiers == '':
            col_modifiers = None
        return RecordsSchemaSqlFieldRepresentation(col_ddl=col_ddl,
                                                   rep_type=rep_type,
                                                   col_type=col_type,
                                                   col_modifiers=col_modifiers)

    @abstractmethod
    def to_data(self) -> 'FieldRepresentationDict':
        raise NotImplementedError


class RecordsSchemaSqlFieldRepresentation(RecordsSchemaFieldRepresentation):
    def __init__(self,
                 col_ddl: str,
                 rep_type: str,
                 col_type: Optional[str],
                 col_modifiers: Optional[str]):
        self.rep_type = rep_type
        self.col_ddl = col_ddl
        self.col_type = col_type
        self.col_modifiers = col_modifiers

    def to_data(self) -> 'SqlFieldRepresentationDict':
        out: 'SqlFieldRepresentationDict' = {
            'rep_type': self.rep_type,
            'col_ddl': self.col_ddl
        }
        if self.col_type is not None:
            out['col_type'] = self.col_type
        if self.col_modifiers is not None:
            out['col_modifiers'] = self.col_modifiers
        return out


class RecordsSchemaPandasFieldRepresentation(RecordsSchemaFieldRepresentation):
    def __init__(self,
                 pd_df_dtype: Dict[str, Any],
                 pd_df_ftype: Optional[str],
                 pd_df_coltype: Union['Literal["series"]', 'Literal["index"]']):
        self.pd_df_dtype = pd_df_dtype
        self.pd_df_ftype = pd_df_ftype
        self.pd_df_coltype = pd_df_coltype

    def to_data(self) -> 'PandasFieldRepresentationDict':
        out: 'PandasFieldRepresentationDict' = {
            'rep_type': 'dataframe/pandas',
            'pd_df_dtype': self.pd_df_dtype,
            'pd_df_coltype': self.pd_df_coltype,
        }
        if self.pd_df_ftype is not None:
            out['pd_df_ftype'] = self.pd_df_ftype
        return out
