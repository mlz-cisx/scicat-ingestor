Simple scicat ingestor
==================

Installation:
pip install -e .


Usage:
export SCICAT_URL= <your scicat url> (should end with `/`)

1) user/pw
export SCICAT_USER=< your username>
export SCICAT_PASSWORD=<your-pw>

2) scicat token
export SCICAT_TOKEN=<your token>

python src/ingestor.py

