FROM python:3.9.11-bullseye


COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN useradd -m docker-user
USER docker-user

WORKDIR /home/docker-user

COPY aleph_scoring/. aleph_scoring/.

CMD [ "python", "-m", "aleph_scoring", "run-on-schedule"]