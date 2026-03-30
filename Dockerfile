FROM docker.io/library/python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y git
RUN mkdir /tmp/src
COPY . /tmp/src
COPY entrypoint.sh /bin/entrypoint.sh

RUN pip install /tmp/src

ENTRYPOINT ["/bin/entrypoint.sh"]
