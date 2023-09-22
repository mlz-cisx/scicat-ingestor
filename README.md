Simple scicat ingestor
======================

Installation
------------

development (assumes you have suitable venv[1]):
```
pip install -e .
```

Usage
-----


Using scicat token (you can get from your profile), currently required for oidc-only instances:

```
export SCICAT_URL= <your scicat url> (should end with `/`)
export SCICAT_TOKEN=<your token>

ingestor
```


Using functional accounts:

```
export SCICAT_USER=< your username>
export SCICAT_PASSWORD=<your-pw>
ingestor
```

[1] venv creation can be done using pyenv with pyenv-virtualenv plugin.
 https://github.com/pyenv/pyenv-virtualenv