#!/usr/bin/env bash

export SOURCE_FOLDER_HOST="<your_source_folder_host>"
export SCICAT_URL="https://scicat.dev.ocp.jcns.frm2.tum.de"
export SCICAT_AUTH_STRATEGY="ldap"
export SCICAT_USER="<user>"
export SCICAT_PASSWORD="<pw>"
export RMQ_HOST="<your rmq host>"
#export RMQ_QUEUE_PORT="5672"
#export RMQ_STREAM_PORT="5552"
export RMQ_VHOST="/"
export RMQ_USER="<user>"
export RMQ_PASSWORD="<pw>"
export RMQ_STREAM="ingestor-test"

#read -s -p "Scicat Password:" SCICAT_PASSWORD
# python src/ingestor.py
python -m ingestor
