MLZ SciCat Ingestor
===================

This is the ingestor used for automatic entry of datasets into the MLZ SciCat catalogue from our instrument control system NICOS[1].
For this, it connects to a RabbitMQ stream and extracts the relevant information from the messages.
Example messages can be found in `examples/messages.txt`.
Currently, login into SciCat is possible with functional accounts or over LDAP. For OIDC-only instances a token has to be provided.

It is under active development and the interfaces may change.

Installation
------------

development (assumes you have suitable venv[2]):

```
pip install -e .
```

Usage
-----

To connect to RabbitMQ the following settings have to be provided:


- `RMQ_HOST=<host>` RabbitMQ host to connect to.
- `RMQ_STREAM_PORT="5552"` Port to connect to RabbitMQ streams on. Defaults to 5552.
- `RMQ_VHOST="/"` Vhost the queue is on.
- `RMQ_STREAM=<stream name>` Stream to read messages from.
- `RMQ_USER=<username>` User to authenticate to RabbitMQ.
- `RMQ_PASSWORD=<password>` Password to authenticate to RabbitMQ.
- `RMQ_DLX_STREAM=<stream name>` Dead-Letter Exchange stream to put messages that produced an error when processing.
- `RMQ_WORKQUEUE=<queue name>` (Optional) Configure to have the ingestor send messages for every file found in the messages. Currently used at MLZ for copying the files from the instruments to the central storage.
- `RMQ_QUEUE_PORT="5672"` (Optional) The port to connect to RabbitMQ queues on. Only needed when the workqueue should be used. Defaults to 5672.

Then, to connect to SciCat using a scicat token (you can get from your profile), currently required for oidc-only instances:

```
export SCICAT_URL= <your scicat url (should end with `/`)>
export SCICAT_TOKEN=<your token>

ingestor
```


Or, when using functional accounts or LDAP (together with the RabbitMQ settings):

```
export SCICAT_URL= <your scicat url (should end with `/`)>
export SCICAT_USER=<your username>
export SCICAT_PASSWORD=<your pw>
export SCICAT_AUTH_STRATEGY=<"ldap" or "login">

ingestor
```

You can provide these settings as environment variables, or in an `.env` file.

There also is a Dockerfile for building a suitable image. You have to provide these settings when starting the container as well.

Access requirements
==================

To be allowed to create datasets, one of the following must be true:

 - The user must be a member of a group in the configured `CREATE_DATASET_GROUPS` to create a dataset and
the owner group of the created dataset must be a group the user is a member of.
 - The user is a member of an admin group.

[1] https://nicos-controls.org
[2] venv creation can be done using pyenv with pyenv-virtualenv plugin.
 https://github.com/pyenv/pyenv-virtualenv
