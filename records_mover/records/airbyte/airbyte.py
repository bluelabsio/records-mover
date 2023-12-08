import logging
from http import HTTPStatus
import requests
from records_mover import Session

logger = logging.getLogger(__name__)


class AirbyteEngine:
    """
    Intention: Main engine of activity for connecting to and managing airflow "stuff."
    General thoughts, maybe this should encapsulate making a request to airbyte
        Thus we'd have a method for making a request which'd know how to authenticate
    """
    session: Session

    def __init__(self, session: Session):
        """
        Args:
            session: Optional records mover Session, exposed for testing.
                     If a session isn't provided, one will be requested
        """
        if session is None:
            self.session = Session()
        else:
            self.session = session

    def healthcheck(self) -> bool:
        self.session.set_stream_logging()
        data = self.session.creds.airbyte()
        if data is None:
            logger.error("Could not retrieve credentials from secrets store")
            return False
        url = f"{data['host']}:{data['port']}/health"
        username = data['user']
        password = data['password']
        try:
            response = requests.get(url, auth=(username, password))
            logger.debug(f"""Airbyte instance {data['host']}. HTTP status code
                                {response.status_code}""")
            if response.status_code is HTTPStatus.OK.value:
                return True
            else:
                return False
        except Exception as e:
            logger.error(f"""Exception encountered executing HTTP request against configured
                             airbyte instance: {e}""")
            return False
