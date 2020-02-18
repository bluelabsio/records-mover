import sys
import logging


stdout_handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(message)s', '%H:%M:%S')
stdout_handler.setFormatter(formatter)


def basic_config() -> None:
    """Set up standard job logging for CLI use.

    Sends any log statements to stdout, with a relatively short
    timestamp beforehand (helpful for timing long-running DS work).
    """
    root_logger = logging.getLogger()
    root_logger.addHandler(stdout_handler)
