FROM python:3.9-slim-bullseye

# GCC is required to compile pyasn, git to install aleph-client with a git tag
RUN apt-get update && apt-get -y upgrade && apt-get install -y \
    build-essential \
    python3-dev \
    git \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

COPY Pipfile /opt/Pipfile
COPY Pipfile.lock /opt/Pipfile.lock

RUN useradd --create-home source
RUN mkdir /opt/.venv
RUN chown source:source /opt/.venv

COPY aleph_scoring/. /opt/aleph_scoring/.
RUN chown --recursive source:source /opt/aleph_scoring

USER source
#RUN pip install --upgrade pip
RUN python3 -m venv /opt/aleph_scoring/.venv
RUN pip install --user pipenv

WORKDIR /opt/

ENV PIPENV_VENV_IN_PROJECT 1
RUN /home/source/.local/bin/pipenv sync

USER root
RUN useradd --create-home user
RUN mkdir /exports
RUN chown user:user /exports

USER user
WORKDIR /opt/

VOLUME "/srv/asn"

ENTRYPOINT ["/opt/.venv/bin/python", "-m", "aleph_scoring"]
CMD ["measure-on-schedule", "--publish"]
