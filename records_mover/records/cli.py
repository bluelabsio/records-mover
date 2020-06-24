"""CLI to move records from place to place"""
import argparse
from odictliteral import odict
from .job.schema import method_to_json_schema
from .job.mover import run_records_mover_job
from ..utils.json_schema import method_signature_to_json_schema
from .processing_instructions import ProcessingInstructions
from records_mover.cli.job_config_schema_as_args_parser import (JobConfigSchemaAsArgsParser,
                                                                arguments_output_to_config)
from records_mover.logging import set_stream_logging
from ..mover_types import JsonSchema, JobConfig
from ..version import __version__
import sys
from typing import Callable, Dict, Any, TYPE_CHECKING
if TYPE_CHECKING:
    from records_mover import Session


def populate_subparser(bootstrap_session: 'Session',
                       sub_parser: argparse.ArgumentParser,
                       source_method_name: str,
                       target_method_name: str,
                       subjob_name: str) -> JobConfig:
    source_method = getattr(bootstrap_session.records.sources, source_method_name)
    target_method = getattr(bootstrap_session.records.targets, target_method_name)
    job_config_schema = {
        "type": "object",
        "properties": odict[
            'source': method_to_json_schema(source_method),  # type: ignore
            'target': method_to_json_schema(target_method),  # type: ignore
        ],
        "required": ["source", "target"],
    }
    JobConfigSchemaAsArgsParser(config_json_schema=job_config_schema,
                                argument_parser=sub_parser).configure_arg_parser()
    return job_config_schema


def make_job_fn(source_method_name: str,
                target_method_name: str,
                name: str,
                job_config_schema: JsonSchema) -> Callable[[Dict[str, Any]], None]:
    def job_fn(raw_config: Dict[str, Any]) -> None:
        job_config = arguments_output_to_config(raw_config)
        run_records_mover_job(source_method_name,
                              target_method_name,
                              job_name=name,
                              config=job_config)
    return job_fn


def main() -> None:
    # https://github.com/googleapis/google-auth-library-python/issues/271
    import warnings
    warnings.filterwarnings("ignore",
                            "Your application has authenticated using end user credentials")

    # skip in-memory sources/targets like dataframes that don't make
    # sense from the command-line
    source_method_name_by_cli_name = {
        'table': 'table',
        'gsheet': 'google_sheet',
        'recordsdir': 'directory_from_url',
        'url': 'data_url',
        'file': 'local_file'
    }
    target_method_name_by_cli_name = {
        'gsheet': 'google_sheet',
        'table': 'table',
        'recordsdir': 'directory_from_url',
        'url': 'data_url',
        'file': 'local_file',
        'spectrum': 'spectrum',
    }
    sources = source_method_name_by_cli_name.keys()
    targets = target_method_name_by_cli_name.keys()

    description = 'Move tabular data ("records") from one place to another'
    parser = argparse.ArgumentParser(description=description,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    pi_config_schema =\
        method_signature_to_json_schema(ProcessingInstructions.__init__,
                                        special_handling={},
                                        parameters_to_ignore=['self'])
    JobConfigSchemaAsArgsParser(config_json_schema=pi_config_schema,
                                argument_parser=parser).configure_arg_parser()

    # https://stackoverflow.com/questions/15405636/pythons-argparse-to-show-programs-version-with-prog-and-version-string-formatt
    parser.add_argument('-V', '--version', action='version', version="%(prog)s ("+__version__+")")
    subparsers = parser.add_subparsers(help='subcommand_help')
    from records_mover import Session
    bootstrap_session = Session()

    for source in sources:
        for target in targets:
            name = f"{source}2{target}"
            sub_parser = subparsers.add_parser(name, help=f"Copy from {source} to {target}")
            source_method_name = source_method_name_by_cli_name[source]
            target_method_name = target_method_name_by_cli_name[target]
            job_config_schema = \
                populate_subparser(bootstrap_session,
                                   sub_parser, source_method_name, target_method_name,
                                   subjob_name=name)
            sub_parser.set_defaults(func=make_job_fn(source_method_name=source_method_name,
                                                     target_method_name=target_method_name,
                                                     name=name,
                                                     job_config_schema=job_config_schema))
    args = parser.parse_args()
    raw_config = vars(args)
    func = getattr(args, 'func', None)
    if func is None:
        parser.print_help()
    else:
        set_stream_logging()
        try:
            func(raw_config)
        except Exception:
            # This is logged above using a redacting logger
            sys.exit(1)
