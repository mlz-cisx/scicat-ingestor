#!/bin/bash
set -e

if [[ $# -eq 0 ]] ; then
    exec /usr/local/bin/ingestor
fi

exec "$@"

