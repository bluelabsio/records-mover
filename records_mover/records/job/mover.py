"""Create and run jobs to convert between different sources and targets"""
from records_mover import Session
from ..processing_instructions import ProcessingInstructions
from ..results import MoveResult
from ...mover_types import JobConfig
from .config import config_to_args
import logging

logger = logging.getLogger(__name__)


def run_records_mover_job(source_method_name: str,
                          target_method_name: str,
                          job_name: str,
                          config: JobConfig) -> MoveResult:
    session = Session()
    try:
        source_method = getattr(session.records.sources, source_method_name)
        target_method = getattr(session.records.targets, target_method_name)
        logger.info('Starting...')

        source_kwargs = config_to_args(config=config['source'],
                                       method=source_method,
                                       session=session)
        target_kwargs = config_to_args(config=config['target'],
                                       method=target_method,
                                       session=session)
        pi_config = {k: config[k] for k in config if k not in ['source', 'target', 'func']}
        pi_kwargs = config_to_args(pi_config,
                                   method=ProcessingInstructions,
                                   session=session)
        processing_instructions = ProcessingInstructions(**pi_kwargs)

        records = session.records
        source = source_method(**source_kwargs)
        target = target_method(**target_kwargs)
        return records.move(source, target, processing_instructions)
    except Exception:
        logger.error('', exc_info=True)
        raise
