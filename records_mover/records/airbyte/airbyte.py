import logging
from http import HTTPStatus

import requests
from records_mover import Session

logger = logging.getLogger(__name__)


def airbyte_healthcheck() -> bool:
    # Steps
    # 1. Retrieve secret via db-facts for airbyte instance info
    session = Session(session_type="lpass")
    session.set_stream_logging()
    data = session.creds.airbyte("local-airbyte")
    if data is None:
        logger.error("Could not retrieve credentials from secrets store")
    url = f"{data['host']}:{data['port']}/health"
    username = data['user']
    password = data['password']
    try:
        response = requests.get(url, auth=(username, password))
        return response.status_code is HTTPStatus.OK
    except Exception as e:
        logger.error(f"Exception encountered executing HTTP request against configured airbyte instance: {e}")
        return False
