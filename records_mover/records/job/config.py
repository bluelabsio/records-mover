"""Create and run jobs to convert between different sources and targets"""
import inspect
from typing import Any, Dict, List, Callable
from records_mover import Session
from ..records_format import DelimitedRecordsFormat, ParquetRecordsFormat, BaseRecordsFormat
from ..existing_table_handling import ExistingTableHandling
from ..delimited import PartialRecordsHints
from records_mover.records.delimited.types import HINT_NAMES
from ...mover_types import JobConfig


class ConfigToArgs:
    def __init__(self,
                 config: JobConfig,
                 method: Callable[..., Any],
                 session: Session):
        self.config = config
        self.method = method
        self.session = session

    def all_arg_names(self, fn: Callable[..., Any]) -> List[str]:
        return [param_name
                for param_name, parameter in inspect.signature(fn).parameters.items()]

    def non_default_arg_names(self, fn: Callable[..., Any]) -> List[str]:
        return [param_name
                for param_name, parameter in inspect.signature(fn).parameters.items()
                if parameter.default == inspect.Parameter.empty]

    def add_hint_parameter(self, kwargs: Dict[str, Any], param_name: str) -> None:
        if 'records_format' in kwargs:
            existing_records_format = kwargs['records_format']
            if not isinstance(existing_records_format, DelimitedRecordsFormat):
                raise NotImplementedError('Hints are not compatible '
                                          'with records format '
                                          f'{existing_records_format.format_type}')
            # records_format already defined by user - populate that
            kwargs['records_format'] = existing_records_format.\
                alter_hints({param_name: kwargs[param_name]})
        elif 'initial_hints' in self.possible_args:
            if 'initial_hints' not in kwargs:
                kwargs['initial_hints'] = {}
            kwargs['initial_hints'][param_name] = kwargs[param_name]
        elif 'records_format' in self.possible_args:
            # user must want a delimited records format - e.g., on
            # output where initial hints are not a thing as we're not
            # auto-detecting - but we haven't gotten their preferred
            # variant yet.  Let's assume the default variant to start -
            # maybe a future arg will reveal a more specific variant.
            kwargs['records_format'] =\
                DelimitedRecordsFormat(hints={param_name: kwargs[param_name]})  # type: ignore
        else:
            raise NotImplementedError(f"Could not find place for {param_name} in "
                                      f"{self.possible_args}")
        del kwargs[param_name]

    def fill_in_spectrum_base_url(self, kwargs: Dict[str, Any]) -> None:
        db_name = kwargs['db_name']
        db_facts = self.session.creds.db_facts(db_name)
        schema_name = kwargs['schema_name']
        key = f"redshift_spectrum_base_url_{schema_name}"
        if key not in db_facts:
            raise ValueError("Please specify spectrum_base_url")
        else:
            kwargs['spectrum_base_url'] = db_facts[key]  # type: ignore

    def fill_in_google_cloud_creds(self, kwargs: Dict[str, Any]) -> None:
        kwargs['google_cloud_creds'] =\
            self.session.creds.google_sheets(self.config['gcp_creds_name'])
        del kwargs['gcp_creds_name']

    def fill_in_existing_table_handling(self, kwargs: Dict[str, Any]) -> None:
        kwargs['existing_table_handling'] =\
            ExistingTableHandling[kwargs['existing_table'].upper()]
        del kwargs['existing_table']

    def fill_in_db_engine(self, kwargs: Dict[str, Any]) -> None:
        kwargs['db_engine'] = self.session.get_db_engine(kwargs['db_name'])

    def fill_in_format(self, kwargs: Dict[str, Any]) -> None:
        records_format: BaseRecordsFormat
        if kwargs['format'] == 'delimited':
            records_format = DelimitedRecordsFormat()
        elif kwargs['format'] == 'parquet':
            records_format = ParquetRecordsFormat()
        else:
            raise NotImplementedError(f"No such records format type: {kwargs['format']}")

        if 'records_format' in kwargs:
            existing_records_format = kwargs['records_format']
            if type(records_format) != type(existing_records_format):
                raise NotImplementedError('Hints are not compatible '
                                          'with records format '
                                          f'{existing_records_format.format_type}')
        else:
            kwargs['records_format'] = records_format

        del kwargs['format']

    def fill_in_variant(self, kwargs: Dict[str, Any]) -> None:
        if 'records_format' in kwargs:
            # We've already started filling this out with hints -
            # but now we know which variant they want
            kwargs['records_format'] = kwargs['records_format'].alter_variant(kwargs['variant'])
        else:
            hints: PartialRecordsHints = {}
            if 'initial_hints' in kwargs:
                # Given the user gave us a variant, we won't be
                # using "initial hints" to sniff with - the hint
                # parameters they gave us should be specified as
                # part of the records format instead.
                hints = kwargs['initial_hints']
                del kwargs['initial_hints']
            kwargs['records_format'] = DelimitedRecordsFormat(variant=kwargs['variant'],
                                                              hints=hints)
        del kwargs['variant']

    def to_args(self) -> Dict[str, Any]:
        kwargs = {}
        for name, value in self.config.items():
            kwargs[name] = value
        self.possible_args = self.all_arg_names(self.method)
        self.expected_args = self.non_default_arg_names(self.method)
        self.missing_args = self.expected_args - kwargs.keys()

        if 'spectrum_base_url' in self.possible_args and kwargs.get('spectrum_base_url') is None:
            self.fill_in_spectrum_base_url(kwargs)

        for arg in self.missing_args:
            if arg == 'self':
                continue
            elif arg == 'google_cloud_creds':
                self.fill_in_google_cloud_creds(kwargs)
            elif arg == 'db_engine':
                self.fill_in_db_engine(kwargs)
            else:
                raise NotImplementedError(f"Did not know how to handle {arg} - "
                                          f"got config {self.config}")
        if 'db_name' in kwargs:
            # Already used to feed different args above
            del kwargs['db_name']
        # for unit test coverage stability, this needs to be a list
        # comprehension (which maintains the order of the kwargs keys)
        # instead of subtracting possible_args from kwargs.keys(),
        # which becomes a set, whose order will vary when iterated
        # over from test run to test run.
        extra_args = [kwarg for kwarg in kwargs if kwarg not in self.possible_args]

        for arg in extra_args:
            if arg == 'existing_table':
                self.fill_in_existing_table_handling(kwargs)
            elif arg == 'variant':
                self.fill_in_variant(kwargs)
            elif arg == 'format':
                self.fill_in_format(kwargs)
            elif arg in HINT_NAMES:
                self.add_hint_parameter(kwargs, arg)
            else:
                raise NotImplementedError(f"Teach me how to pass in {arg}")
        return kwargs


def config_to_args(config: JobConfig,
                   method: Callable[..., Any],
                   session: Session) -> Dict[str, Any]:
    return ConfigToArgs(config, method, session).to_args()
