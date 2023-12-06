FROM docker.ictrl.frm2.tum.de:5443/docker_proxy/library/python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y git
RUN mkdir /tmp/src
COPY . /tmp/src

RUN pip install /tmp/src

ENTRYPOINT ["ingestor"]
