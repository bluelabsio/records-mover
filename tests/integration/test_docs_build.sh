#!/bin/bash
set -x
export PYENV_VERSION=3.8.16

TEST_VENV_NAME=test-docs-sphinx-build
SCRIPT_DIR=$(dirname $0)
DOCS_DIR=${SCRIPT_DIR}/../../docs

pyenv install -s ${PYENV_VERSION}

pyenv virtualenv-delete -f ${TEST_VENV_NAME}
pyenv virtualenv ${TEST_VENV_NAME}
. ${PYENV_ROOT}/versions/${TEST_VENV_NAME}/bin/activate

pip install -r ${DOCS_DIR}/source/sphinx_requirements.txt

cd ${DOCS_DIR}
make clean
make html

unset PYENV_VERSION
