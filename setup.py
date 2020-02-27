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

__version__: str
# Read in and set version variable without the overhead/requirements
# of the rest of the package.
#
# https://milkr.io/kfei/5-common-patterns-to-version-your-Python-package/5
dir_path = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(dir_path, 'records_mover', 'version.py')) as f:
    exec(f.read())

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
          'pybigquery',
          'sqlalchemy',
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
