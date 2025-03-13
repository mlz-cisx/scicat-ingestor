#!/usr/bin/env bash

export SCICAT_URL="https://scicat.dev.ocp.jcns.frm2.tum.de"
export SCICAT_AUTH_STRATEGY="ldap"
export SCICAT_USER="workbench"
export SCICAT_PASSWORD="w0rkb3nc4"
export RMQ_HOST="infra01.dev.cluster.jcns.frm2.tum.de"
export RMQ_PORT="31985"
export RMQ_VHOST="/"
export RMQ_USER="workbench"
export RMQ_PASSWORD="w0rkb3nc4"
export RMQ_STREAM="ingestor-test"

#read -s -p "Scicat Password:" SCICAT_PASSWORD
echo
# python src/ingestor.py
python -m ingestor
