import os
import logging


def adjusted_log_level(default_log_level: int, app_name: str) -> int:
    log_level_str = os.environ.get(app_name.upper() + '_LOG_LEVEL', None)
    if log_level_str is None:
        return default_log_level
    return getattr(logging, log_level_str.upper())


def set_levels(app_name: str,
               default_app_log_level: int=logging.INFO,
               default_root_log_level: int=logging.INFO) -> None:
    """
    Set up standard job log levels.  Can be overridden by
    YOUR_JOB_NAME_LOG_LEVEL env variable set to INFO/WARNING/etc

    :param app_name: module name of application; job logger is mounted here
    :param default_app_log_level: level of logging from application (default INFO)
    :param default_root_log_level: level of logging from root (default INFO)
    """

    # Log all messages to stderr
    root_logger = logging.getLogger()
    root_logger.setLevel(adjusted_log_level(default_root_log_level, app_name))

    # Log app messages to job_log by default
    app_logger = logging.getLogger(app_name)
    app_logger.setLevel(adjusted_log_level(default_app_log_level, app_name))
