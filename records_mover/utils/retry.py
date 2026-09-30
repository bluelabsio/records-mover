from sqlalchemy.exc import DatabaseError
import tenacity
import logging

logger = logging.getLogger(__name__)


def bigquery_retry():

    def is_bigquery_rate_limit_exception(e: Exception) -> bool:
        return ('403 Exceeded rate limits:' in str(e) or
                'Job exceeded rate limits' in str(e))

    # Example raised when we exceed BigQuery API rate limits:

    # sqlalchemy.exc.DatabaseError: (google.cloud.bigquery.dbapi.exceptions.DatabaseError)
    # 403 Exceeded rate limits: too many table update operations for this table. For more
    # information, see https://cloud.google.com/bigquery/troubleshooting-errors
    return tenacity.retry(wait=tenacity.wait_random_exponential(multiplier=1, max=60),
                          stop=tenacity.stop_after_attempt(5),
                          before_sleep=tenacity.before_sleep_log(logger, logging.WARNING),
                          retry=(tenacity.retry_if_exception_type(DatabaseError) &
                                 # Wish the specific error were tied to the type or a field...
                                 #
                                 # https://google-cloud-python.readthedocs.io/en/0.32.0/_modules/google/cloud/bigquery/dbapi/exceptions.html#DatabaseError
                                 tenacity.retry_if_exception(is_bigquery_rate_limit_exception)),
                          reraise=True)
