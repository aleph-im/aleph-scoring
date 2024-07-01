FROM python:3.11-slim-bookworm

# GCC is required to compile pyasn, git to install aleph-client with a git tag
RUN apt-get update && apt-get -y upgrade && apt-get install -y \
    build-essential \
    python3-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user for the installation of dependencies in a virtual environment
RUN useradd --create-home source
RUN mkdir /opt/venv
RUN chown source:source /opt/venv
USER source
RUN python3 -m venv /opt/venv
RUN /opt/venv/bin/pip install --upgrade pip hatch hatch-vcs

# Copy the source code
USER root
RUN mkdir /opt/aleph-scoring
WORKDIR /opt/aleph-scoring

COPY ./pyproject.toml .
COPY ./src .
COPY .git .git
COPY LICENSE.txt .
COPY README.md .

# Install the package in the virtual environment
USER source
WORKDIR /opt/
RUN /opt/venv/bin/pip install --editable /opt/aleph-scoring

# Create a non-root user for the execution of the application
USER root
RUN useradd --create-home user
RUN mkdir /exports
RUN chown user:user /exports

USER user
WORKDIR /home/user/

VOLUME "/srv/asn"
ENV ALEPH_SCORING_ASN_DB_DIRECTORY "/srv/asn"

ENTRYPOINT ["/opt/venv/bin/python", "-m", "aleph_scoring"]
CMD ["measure-on-schedule", "--publish"]
