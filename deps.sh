#!/bin/bash -e

# don't use GPLed dependency
# https://medium.com/datareply/apache-airflow-1-10-0-released-highlights-6bbe7a37a8e1
export SLUGIFY_USES_TEXT_UNIDECODE=yes

brew update && ( brew upgrade pyenv || true )
pyenv rehash  # needed if pyenv is updated

python_version=3.9.0
# zipimport.ZipImportError: can't decompress data; zlib not available:
#    You may need `xcode-select --install` on OS X
#    https://github.com/pyenv/pyenv/issues/451#issuecomment-151336786
pyenv install -s "${python_version:?}"
pyenv virtualenv "${python_version:?}" records-mover-"${python_version:?}" || true
pyenv local records-mover-"${python_version:?}"

pip3 install --upgrade pip
#
# It's nice to unit test, integration test, and run the CLI in
# a development pyenv.
#
pip3 install -r requirements.txt -e '.[unittest,itest,cli,typecheck,docs]'
