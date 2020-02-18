#!/usr/bin/env python

import os
from setuptools import setup, find_packages
from setuputils import TestCoverageRatchetCommand, MypyCoverageRatchetCommand, VerifyVersionCommand

gsheet_dependencies = [
    'google',
    'google_auth_httplib2',
    'google-api-python-client>=1.5.0,<1.6.0',
    'oauth2client>=2.0.2,<2.1.0',
    'PyOpenSSL'
]

SQL_ALCHEMY_VERTICA_PYTHON_URL = (
    'git+https://github.com/vinceatbluelabs/'
    'sqlalchemy-vertica-python.git@columns_in_order#egg=sqlalchemy-vertica-python-0.4.5'
)

__version__: str
# Read in and set version variable without the overhead/requirements
# of the rest of the package.
#
# https://milkr.io/kfei/5-common-patterns-to-version-your-Python-package/5
dir_path = os.path.dirname(os.path.realpath(__file__))
exec(open(os.path.join(dir_path, 'records_mover', 'version.py')).read())

setup(name='records-mover',
      version=__version__, # read right above  # noqa
      description='BlueLabs Job Library for Python',
      author='Vince Broz',
      author_email='vince.broz@bluelabs.com',
      packages=find_packages(),
      package_data={
          'records_mover': ['py.typed']
      },
      install_requires=[
          'boto>=2,<3', 'boto3',
          'jsonschema', 'timeout_decorator', 'awscli',
          'PyYAML', 'psycopg2',
          # https://github.com/LocusEnergy/sqlalchemy-vertica-python/issues/36
          # https://github.com/vertica/vertica-python/issues/280
          'vertica-python<0.9.2',
          # 'vertica-sqlalchemy>=0.14.3,<0.15.0',
          # Three annoying things:
          #
          # 1. The version of sqlalchemy-vertica-python published in
          #    pipy exports Table objects with the columns in some
          #    arbitrary order because it's missing an ORDER BY clause.
          # 2. The upstream source is no longer accepting PRs:
          # 3. Since the upstream source for the vertica-python-based
          #    sqlalchemy driver is a fork of the vertica-odbc-based
          #    sqlalchemy driver, and we've already forked that one into
          #    the bluelabsio org, we can't fork the vertica-python-based
          #    sqlalchemy driver there. Thus it's under vinceatbluelabs
          #    instead of bluelabsio.
          #
          # https://github.com/LocusEnergy/sqlalchemy-vertica-python/pull/37
          'sqlalchemy-vertica-python @ ' + SQL_ALCHEMY_VERTICA_PYTHON_URL,
          # 0.7.7 introduced support for Parquet in UNLOAD
          'sqlalchemy-redshift>=0.7.7',
          'pybigquery',
          'sqlalchemy',
          # 'pyodbc',
          # Not sure how/if interface will change in db-facts, so
          # let's be conservative about what we're specifying for now.
          'db-facts>=2.15.3,<3',
          'odictliteral',
          # we rely on exception types from smart_open,
          # which seem to change in feature releases
          # without a major version bump
          'smart_open>=1.8.4,<1.9.0',
          'chardet',
          's3-concat>=0.1.7,<0.2'
          'tenacity>=6<7'
      ],
      extras_require={
          'gsheets': gsheet_dependencies,
          'movercli': gsheet_dependencies + ['typing_inspect', 'docstring_parser',
                                             'pandas<2',
                                             'pyarrow'],
      },
      entry_points={
          'console_scripts': 'mvrec = records_mover.records.cli:main',
      },
      cmdclass={
          'coverage_ratchet': TestCoverageRatchetCommand,
          'mypy_ratchet': MypyCoverageRatchetCommand,
          'verify': VerifyVersionCommand,
      })
