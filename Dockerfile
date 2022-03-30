FROM python:3.9-slim-bullseye

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN useradd -m user
RUN mkdir /exports
RUN chown user:user /exports

USER user

WORKDIR /home/user

COPY aleph_scoring/. aleph_scoring/.

ENTRYPOINT ["python", "-m", "aleph_scoring"]
