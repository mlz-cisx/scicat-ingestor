import asyncio
import functools
import os
import signal
import sys
from dataclasses import dataclass

import mlzlog
from dotenv import load_dotenv

from .client import AuthStrategy
from .consumer import RabbitConsumer

# TODO: configurable to reuse the rabbitmq logic part?
from .ingestor import Ingestor
from .util import error_fallback, stop_everything

# import logging
# logging.basicConfig(level=logging.DEBUG)


@dataclass
class RabbitMQConfig:
    host: str
    port: int
    queue_port: int
    vhost: str
    stream: str
    user: str
    pw: str
    dlx_stream: str
    workqueue: str | None = None


@dataclass
class SciCatConfig:
    url: str
    auth_strat: AuthStrategy
    user: str
    pw: str


@dataclass
class Config:
    rmq: RabbitMQConfig
    sc: SciCatConfig
    source_folder_host: str
    enable_md_fallback: bool


def get_config():
    load_dotenv()
    sc_url = os.environ.get("SCICAT_URL")
    if not sc_url:
        raise ValueError("No SciCat url configured")
    sc_url = sc_url.rstrip("/")
    try:
        auth_strategy = os.environ.get("SCICAT_AUTH_STRATEGY", "login")
        auth_strategy = AuthStrategy(auth_strategy)
    except ValueError as e:
        raise ValueError(
            f"Argument {auth_strategy!r} is not a valid authentication strategy. "
            f"Use 'login' or 'ldap'",
        ) from e
    enable_fallback = os.environ.get("ENABLE_MD_FALLBACK", "False").lower() == "true"
    sfh = os.environ.get("SOURCE_FOLDER_HOST")
    if not sfh:
        raise ValueError("No Source folder host configured")
    sc_user = os.environ.get("SCICAT_USER")
    sc_pw = os.environ.get("SCICAT_PASSWORD")
    sc = SciCatConfig(sc_url, auth_strategy, sc_user, sc_pw)
    rmq_host = os.environ.get("RMQ_HOST")
    if not rmq_host:
        raise ValueError("No RabbitMQ url configured")
    rmq_port = int(os.environ.get("RMQ_STREAM_PORT", "5552"))
    rmq_vhost = os.environ.get("RMQ_VHOST", "/")
    rmq_stream = os.environ.get("RMQ_STREAM")
    if not rmq_stream:
        raise ValueError("No RabbitMQ Stream configured for listenning")
    rmq_user = os.environ.get("RMQ_USER")
    if not rmq_user:
        raise ValueError("No RabbitMQ user configured")
    rmq_pw = os.environ.get("RMQ_PASSWORD")
    if not rmq_pw:
        raise ValueError("No RabbitMQ password configured")
    rmq_dlx_stream = os.environ.get("RMQ_DLX_STREAM")
    if not rmq_dlx_stream:
        raise ValueError("No dead-letter exchange stream configured")
    rmq_workqueue = os.environ.get("RMQ_WORKQUEUE")
    rmq_queue_port = int(os.environ.get("RMQ_QUEUE_PORT", "5672"))
    rmq = RabbitMQConfig(rmq_host, rmq_port, rmq_queue_port, rmq_vhost, rmq_stream, rmq_user,
                         rmq_pw, rmq_dlx_stream, rmq_workqueue)
    return Config(rmq, sc, sfh, enable_fallback)


async def _main():
    exit_code = [0]
    cfg = get_config()

    loop = asyncio.get_event_loop()

    handler = functools.partial(stop_everything, loop, exit_code, code=130)
    loop.add_signal_handler(signal.SIGINT, handler, "SIGINT")
    loop.add_signal_handler(signal.SIGTERM, handler, "SIGTERM")
    loop.set_exception_handler(functools.partial(error_fallback, exit_code=exit_code))

    ingestor = Ingestor(cfg, mlzlog.getLogger("ingestor"))
    consumer = RabbitConsumer(cfg.rmq, ingestor, mlzlog.getLogger("consumer"))
    try:
        await consumer.run()
    except asyncio.CancelledError:
        mlzlog.log.info("stopped")
    return exit_code[0]


def main():
    mlzlog.initLogging("ingest", rootlevel="info",
                       logdir=None)  # only to console
    exit_code = asyncio.run(_main())
    sys.exit(exit_code)
