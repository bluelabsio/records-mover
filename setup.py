#!/usr/bin/env python

import os
from setuptools import setup, find_packages
from setuptools.command.install import install
from typing import Optional
from decimal import Decimal
from distutils.cmd import Command
import os.path
import sys

__version__: Optional[str] = None
# Read in and set version variable without the overhead/requirements
# of the rest of the package.
#
# https://milkr.io/kfei/5-common-patterns-to-version-your-Python-package/5
dir_path = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(dir_path, 'records_mover', 'version.py')) as f:
    exec(f.read())
    assert __version__ is not None


# From https://circleci.com/blog/continuously-deploying-python-packages-to-pypi-with-circleci/
class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version"""
    description = 'verify that the git tag matches our version'

    def run(self) -> None:
        tag = os.getenv('CIRCLE_TAG')
        tag_formatted_version = f'v{__version__}'

        if tag != tag_formatted_version:
            info = "Git tag: {0} does not match the version of this app: {1}".format(
                tag, __version__
            )
            sys.exit(info)


class CoverageRatchetCommand(Command):
    description = 'Run coverage ratchet'
    user_options = []  # type: ignore
    coverage_file: str
    coverage_source_file: str
    coverage_url: str
    type_of_coverage: str

    def finalize_options(self) -> None:
        pass

    def run(self) -> None:
        """Run command."""
        import xml.etree.ElementTree as ET

        tree = ET.parse(self.coverage_source_file)
        new_coverage = Decimal(tree.getroot().attrib["line-rate"]) * 100

        if not os.path.exists(self.coverage_file):
            with open(self.coverage_file, 'w') as f:
                f.write('0')

        with open(self.coverage_file, 'r') as f:
            high_water_mark = Decimal(f.read())

        if new_coverage < high_water_mark:
            raise Exception(
                f"{self.type_of_coverage} coverage used to be {high_water_mark}; "
                f"down to {new_coverage}%.  Fix by viewing '{self.coverage_url}'")
        elif new_coverage > high_water_mark:
            with open(self.coverage_file, 'w') as f:
                f.write(str(new_coverage))
            print(f"Just ratcheted coverage up to {new_coverage}%")
        else:
            print(f"Code coverage steady at {new_coverage}%")


class TestCoverageRatchetCommand(CoverageRatchetCommand):
    def initialize_options(self) -> None:
        """Set default values for options."""
        self.type_of_coverage = 'Test'
        self.coverage_url = 'cover/index.html'
        self.coverage_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'metrics',
            'coverage_high_water_mark'
        )
        self.coverage_source_file = "coverage.xml"


class MypyCoverageRatchetCommand(CoverageRatchetCommand):
    def initialize_options(self) -> None:
        """Set default values for options."""
        self.type_of_coverage = 'Mypy'
        self.coverage_url = 'typecover/index.html'
        self.coverage_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'metrics',
            'mypy_high_water_mark'
        )
        self.coverage_source_file = "typecover/cobertura.xml"


google_api_client_dependencies = [
    # 1.8 seems to be required to use application default creds with
    # 'googleapiclient.discovery.build':
    #
    # https://github.com/googleapis/google-auth-library-python/issues/190
    'google-api-python-client>=1.8.0,<1.9.0',
    #
    # For some reason this issue started happening consistently around 2020-10:
    #
    #  https://app.circleci.com/pipelines/github/bluelabsio/records-mover/1134/workflows/267ca651-def1-4f60-bdfb-0f857a9e0c60/jobs/9770
    #
    # This seems to be a clue:
    #
    #  https://github.com/pypa/pip/issues/8407
    #
    # However, downgrading pip does not seem to resolve it, so it
    # doesn't seem to have been caused by a pip upgrade.  For now
    # we'll explicitly state the dependency for force the install:
    'grpcio<2.0dev,>=1.29.0',
]

nose_dependencies = [
    'nose'
]

itest_dependencies = [
    'jsonschema',  # needed for directory_validator.py
] + (
    nose_dependencies +
    # needed for records_database_fixture retrying drop/creates on
    # BigQuery
    google_api_client_dependencies
)

airflow_dependencies = [
    # Minimum version here is needed to avoid syntax error in setup.py
    # in 1.10.0
    'apache-airflow>=1.10.1,<2'
]

db_dependencies = [
    # Lower bound (>=1.3.18) is to improve package resolution performance
    #
    # Upper bound (<1.4) is to avoid 1.4 which has breaking changes and is
    # incompatible with python-bigquery-sqlalchemy per
    # https://github.com/googleapis/python-bigquery-sqlalchemy/issues/83
    # Can lift this once records-mover itself is compatible and
    # other packages have appropriate restrictions in place.
    'sqlalchemy>=1.3.18,<1.4',
]

smart_open_dependencies = [
    # smart_open requires rsa>=3.1.4, which causes pip 20.2.4 in
    # default mode to install rsa 4.6.
    #
    # That then results in this error from pip at install-time:
    #
    # awscli 1.18.178 requires rsa<=4.5.0,>=3.1.2; python_version !=
    # "3.4", but you'll have rsa 4.6 which is incompatible.
    #
    # Also, pipgrip (used to make Python formulas in Homebrew) takes
    # somewhere between much longer and forever to provide its output
    # without this line.
    #
    'rsa>=3.1.4,<=4.5.0',
    # we rely on exception types from smart_open,
    # which seem to change in feature releases
    # without a major version bump
    'smart_open>=2,<2.1',
]

gcs_dependencies = [
    'google-cloud-storage'
] + smart_open_dependencies

bigquery_dependencies = [
    # This is currently vendored in
    # records_mover/db/postgres/sqlalchemy_postgres_copy.py but
    # once this PR is merged and a new version published, we can
    # use the new upstream version:
    #
    # https://github.com/jmcarp/sqlalchemy-postgres-copy/pull/14
    #
    # 'sqlalchemy-postgres-copy>=0.5,<0.6',
    'pybigquery',
] + gcs_dependencies + db_dependencies


aws_dependencies = [
    'awscli>=1,<2',
    'boto>=2,<3',
    'boto3',
    's3-concat>=0.1.7,<0.2'
] + smart_open_dependencies

gsheet_dependencies = [
    'google',
    'google_auth_httplib2',
    'oauth2client>=2.0.2,<2.1.0',
    'PyOpenSSL'
] + google_api_client_dependencies

parquet_dependencies = [
    'pyarrow'
]

pandas_dependencies = [
    'pandas<2',
]

mysql_dependencies = [
    'pymysql'
] + db_dependencies

redshift_dependencies_base = [
    # sqlalchemy-redshift 0.7.7 introduced support for Parquet
    # in UNLOAD
    'sqlalchemy-redshift>=0.7.7',
] + aws_dependencies + db_dependencies

redshift_dependencies_binary = [
    'psycopg2-binary',
] + redshift_dependencies_base

redshift_dependencies_source = [
    'psycopg2',
] + redshift_dependencies_base

postgres_depencencies_base = db_dependencies

postgres_dependencies_binary = [
    'psycopg2-binary',
] + postgres_depencencies_base

postgres_dependencies_source = [
    'psycopg2',
] + postgres_depencencies_base

cli_dependencies_base = [
    'odictliteral',
    'jsonschema',
    'docstring_parser',
]

vertica_dependencies = [
    # sqlalchemy-vertica-python 0.5.5 introduced
    # https://github.com/bluelabsio/sqlalchemy-vertica-python/pull/7
    # which fixed a bug pulling schema information from Vertica
    'sqlalchemy-vertica-python>=0.5.5,<0.6',
] + db_dependencies

literally_every_single_database_binary_dependencies = (
    vertica_dependencies +
    postgres_dependencies_binary +
    redshift_dependencies_binary +
    bigquery_dependencies +
    mysql_dependencies
)

typecheck_dependencies = [
    'mypy==0.790',
    'lxml',  # needed by mypy HTML coverage reporting
    'sqlalchemy-stubs>=0.3',
]

unittest_dependencies = [
    'coverage',
    'mock',
] + (
    nose_dependencies +
    cli_dependencies_base +
    airflow_dependencies +
    gsheet_dependencies +
    literally_every_single_database_binary_dependencies +
    aws_dependencies +
    pandas_dependencies +
    gcs_dependencies
)

docs_dependencies = [
    'sphinx>=3',  # used to generate and upload docs -
                  # sphinx-autodoc-typehints requires 3 or better per
                  # https://github.com/agronholm/sphinx-autodoc-typehints/pull/138
    'sphinx-rtd-theme',  # used to style docs for readthedocs.io
    'recommonmark',  # used to be able to use sphinx with markdown
] + (
    # needed for readthedocs.io to be able to evaluate modules with
    # sqlalchemy imports
    db_dependencies +
    # Same with Airflow
    airflow_dependencies +
    # Also boto
    aws_dependencies
)

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name='records-mover',
      version=__version__, # read right above  # noqa
      description=('Library and CLI to move relational data from one place to another - '
                   'DBs/CSV/gsheets/dataframes/...'),
      long_description=long_description,
      long_description_content_type="text/markdown",
      download_url=f'https://github.com/bluelabsio/records-mover/tarball/{__version__}',  # noqa
      author='Vince Broz',
      author_email='opensource@bluelabs.com',
      packages=find_packages(),
      package_data={
          'records_mover': ['py.typed']
      },
      install_requires=[
          'timeout_decorator',
          # awscli pins PyYAML. We have seen issues at
          # run-time if we don't constrain things here as well. So
          # this tracks the awscli req for python >= 3.6 which is
          # what we support
          #
          # https://github.com/aws/aws-cli/blob/develop/setup.py
          'PyYAML>=3.10,<5.5',
          # Not sure how/if interface will change in db-facts, so
          # let's be conservative about what we're specifying for now.
          'db-facts>=4,<5',
          # Version 4.0.0 is not yet compatible with requests, and pip
          # 20.3 is not smart enough to pay attention to that,
          # resulting in runtime complaints.  Can be removed once 20.4
          # is proven to work.
          'chardet>=3,<4',
          'tenacity>=4.12.0,<7',
          # v5.0.1 resolves https://github.com/exhuma/config_resolver/issues/69
          'config-resolver>=5.0.1,<6',
          'typing_inspect',
          'typing-extensions',
      ],
      extras_require={
          'airflow': airflow_dependencies,
          'db': db_dependencies,
          'gsheets': gsheet_dependencies,
          'cli': cli_dependencies_base,
          'bigquery': bigquery_dependencies,
          'aws': aws_dependencies,
          'mysql': mysql_dependencies,
          'redshift-binary': redshift_dependencies_binary,
          'redshift-source': redshift_dependencies_source,
          'postgres-binary': postgres_dependencies_binary,
          'postgres-source': postgres_dependencies_source,
          'vertica': vertica_dependencies,
          'pandas': pandas_dependencies,
          # don't let it be said we didn't warn you.
          'literally_every_single_database_binary':
          literally_every_single_database_binary_dependencies,
          'itest': itest_dependencies,
          'unittest': unittest_dependencies,
          'typecheck': typecheck_dependencies,
          'gcs': gcs_dependencies,
          'parquet': parquet_dependencies,
          'docs': docs_dependencies,
      },
      entry_points={
          'console_scripts': 'mvrec = records_mover.records.cli:main',
      },
      cmdclass={
          'coverage_ratchet': TestCoverageRatchetCommand,
          'mypy_ratchet': MypyCoverageRatchetCommand,
          'verify': VerifyVersionCommand,
      },
      license='Apache Software License',
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'Topic :: Database :: Front-Ends',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
      ])
