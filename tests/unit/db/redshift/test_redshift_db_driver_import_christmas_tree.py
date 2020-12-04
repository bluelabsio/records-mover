from .base_test_redshift_db_driver import BaseTestRedshiftDBDriver
from records_mover.db.redshift.redshift_db_driver import Table
from mock import call, patch
from records_mover.records.delimited.utils import logger as driver_logger
from sqlalchemy_redshift.commands import Encoding, Compression


class TestRedshiftDBDriverImportBlueLabs(BaseTestRedshiftDBDriver):
    maxDiff = None
