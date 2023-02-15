FROM python:3.9-slim-bullseye

# GCC is required to compile pyasn, git to install aleph-client with a git tag
RUN apt-get update && apt-get -y upgrade && apt-get install -y \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN useradd -m user
RUN mkdir /exports
RUN chown user:user /exports

USER user

WORKDIR /home/user

COPY aleph_scoring/. aleph_scoring/.

ENTRYPOINT ["python", "-m", "aleph_scoring"]
