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


gsheet_dependencies = [
    'google',
    'google_auth_httplib2',
    'google-api-python-client>=1.5.0,<1.6.0',
    'oauth2client>=2.0.2,<2.1.0',
    'PyOpenSSL'
]

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
          'boto>=2,<3', 'boto3',
          'jsonschema', 'timeout_decorator',
          'awscli>=1,<2',
          # awscli pins PyYAML below 5.3 so they can maintain support
          # for old versions of Python.  This can cause issues at
          # run-time if we don't constrain things here as well, as a
          # newer version seems to sneak in:
          #
          # pkg_resources.ContextualVersionConflict:
          #   (PyYAML 5.3 (.../lib/python3.7/site-packages),
          #     Requirement.parse('PyYAML<5.3,>=3.10'), {'awscli'})
          #
          # https://github.com/aws/aws-cli/blob/develop/setup.py
          'PyYAML<5.3',
          # sqlalchemy-vertica-python 0.5.5 introduced
          # https://github.com/bluelabsio/sqlalchemy-vertica-python/pull/7
          # which fixed a bug pulling schema information from Vertica
          'sqlalchemy-vertica-python>=0.5.5,<0.6',
          # sqlalchemy-redshift 0.7.7 introduced support for Parquet
          # in UNLOAD
          'sqlalchemy-redshift>=0.7.7',
          # This is currently vendored in
          # records_mover/db/postgres/sqlalchemy_postgres_copy.py but
          # once this PR is merged and a new version published, we can
          # use the new upstream version:
          #
          # https://github.com/jmcarp/sqlalchemy-postgres-copy/pull/14
          #
          # 'sqlalchemy-postgres-copy>=0.5,<0.6',
          'pybigquery',
          'sqlalchemy',
          # Not sure how/if interface will change in db-facts, so
          # let's be conservative about what we're specifying for now.
          'db-facts>=3,<4',
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
          'movercli': gsheet_dependencies + ['typing_inspect',
                                             'docstring_parser',
                                             'psycopg2-binary',
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
