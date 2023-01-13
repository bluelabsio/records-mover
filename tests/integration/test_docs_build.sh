#!/bin/bash

if [[ $1 == "--help" ]]
then
    echo "Usage: ./test_docs_build.sh [--no-pyenv]"
    echo "If --no-pyenv is given, no virtualenv will be created to hold "
    echo "installed packages."
    exit 0
elif [[ $1 != "--no-pyenv" ]]
then
    echo "Creating virtualenv..."
    export PYENV_VERSION=3.8.16

    TEST_VENV_NAME=test-docs-sphinx-build

    pyenv install -s ${PYENV_VERSION}

    pyenv virtualenv-delete -f ${TEST_VENV_NAME}
    pyenv virtualenv ${TEST_VENV_NAME}
    . ${PYENV_ROOT}/versions/${TEST_VENV_NAME}/bin/activate
fi

echo "Installing sphinx dependencies..."
SCRIPT_DIR=$(dirname $0)
DOCS_DIR=${SCRIPT_DIR}/../../docs

pip install -r ${DOCS_DIR}/source/sphinx_requirements.txt

cd ${DOCS_DIR}
make clean
make html
