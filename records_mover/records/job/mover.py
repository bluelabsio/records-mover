"""Create and run jobs to convert between different sources and targets"""
from records_mover.job_context import create_job_context
from ..processing_instructions import ProcessingInstructions
from ..results import MoveResult
from ...types import JobConfig
from .config import config_to_args


def run_records_mover_job(source_method_name: str,
                          target_method_name: str,
                          job_name: str,
                          config: JobConfig) -> MoveResult:
    with create_job_context(job_name) as job_context:
        try:
            source_method = getattr(job_context.records.sources, source_method_name)
            target_method = getattr(job_context.records.targets, target_method_name)
            job_context.logger.info('Starting...')
            logger = job_context.logger
            logger.debug("Initialized class!")
            records = job_context.records

            source_kwargs = config_to_args(config=config['source'],
                                           method=source_method,
                                           job_context=job_context)
            target_kwargs = config_to_args(config=config['target'],
                                           method=target_method,
                                           job_context=job_context)
            pi_config = {k: config[k] for k in config if k not in ['source', 'target', 'func']}
            pi_kwargs = config_to_args(pi_config,
                                       method=ProcessingInstructions,
                                       job_context=job_context)
            processing_instructions = ProcessingInstructions(**pi_kwargs)

            source = source_method(**source_kwargs)
            target = target_method(**target_kwargs)
            return records.move(source, target, processing_instructions)
        except Exception as e:
            job_context.logger.error(e)
            raise
