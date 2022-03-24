FROM python:3.9.11-bullseye


COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN useradd -m docker-user
USER docker-user

WORKDIR /home/docker-user

COPY app/. app/.

CMD [ "python", "-m", "app.main", "run-on-schedule"]