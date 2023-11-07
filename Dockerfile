FROM cimg/python:3.9
ARG VERSION=3.9.1
ARG NAME=records-mover
ENV version_env=$VERSION
ENV name_env=$NAME
WORKDIR /records-mover
RUN git clone https://github.com/bluelabsio/records-mover.git .
RUN pyenv install $version_env
RUN pyenv virtualenv "$version_env" "$name_env"-"$version_env"
RUN pyenv local records-mover-$version_env
RUN python -m venv venv \
    && . venv/bin/activate \
    && pip install --upgrade pip \
    && pip install --progress-bar=off -r requirements.txt \
    && pip install --upgrade --progress-bar=off 'pandas>=1.5.3,<2' \
    && pip install -e '.[unittest,typecheck]' #\
    && make citest \
    && make cicoverage \
    && make typecheck \
    && make citypecoverage
