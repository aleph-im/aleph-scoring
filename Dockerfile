FROM python:3.12-slim-bookworm

# GCC is required to compile pyasn, git to install aleph-client with a git tag
RUN apt-get update && apt-get -y upgrade && apt-get install -y \
    build-essential \
    python3-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy source code as user 'source':
RUN useradd --create-home source
RUN mkdir /opt/scoring
RUN mkdir /opt/venv
RUN chown source:source /opt/scoring /opt/venv

COPY ./aleph_scoring /opt/scoring/aleph_scoring

COPY ./pyproject.toml /opt/scoring/pyproject.toml
COPY .git /opt/scoring/.git
COPY README.md /opt/scoring/README.md

RUN chown --recursive source:source /opt/scoring

USER source
RUN python3 -m venv /opt/venv

RUN /opt/venv/bin/pip install --upgrade pip
RUN /opt/venv/bin/pip install /opt/scoring/

WORKDIR /opt/scoring

# Setup a different user that runs the scoring:
USER root
RUN useradd --create-home user
RUN mkdir /exports
RUN chown user:user /exports

USER user
WORKDIR /opt/

VOLUME "/srv/asn"

# Test launching the process
RUN /opt/venv/bin/python -m aleph_scoring --help

ENTRYPOINT ["/opt/venv/bin/python", "-m", "aleph_scoring"]
CMD ["measure-on-schedule", "--publish"]
