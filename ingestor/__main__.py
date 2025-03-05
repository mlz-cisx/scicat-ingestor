#!/usr/bin/env python3

import asyncio
import os
import signal
from dataclasses import dataclass

import mlzlog
from dotenv import load_dotenv

from .client import AuthStrategy
from .ingestor import Ingestor
from .message import testmsg


@dataclass
class RabbitMQConfig:
    host: str
    port: int
    vhost: str
    stream: str
    user: str
    pw: str


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


def get_config():
    load_dotenv()
    sc_url = os.environ.get('SCICAT_URL')
    if not sc_url:
        raise ValueError('No SciCat url configured')
    if not sc_url.endswith("/"):
        sc_url += "/"
    try:
        auth_strategy = AuthStrategy(os.environ.get("SCICAT_AUTH_STRATEGY", "login"))
    except ValueError as e:
        raise ValueError(
            f"Argument {auth_strategy!r} is not a valid authentication strategy. "
            f"Use 'login' or 'ldap'"
        ) from e
    sc_user = os.environ.get("SCICAT_USER")
    sc_pw = os.environ.get("SCICAT_PASSWORD")
    sc = SciCatConfig(sc_url, auth_strategy, sc_user, sc_pw)
    rmq_host = os.environ.get("RMQ_HOST")
    if not rmq_host:
        raise ValueError("No RabbitMQ url configured")
    rmq_port = int(os.environ.get("RMQ_PORT", 5552))
    rmq_vhost = os.environ.get("RMQ_VHOST", "/")
    rmq_stream = os.environ.get("RMQ_STREAM")
    if not rmq_stream:
        raise ValueError("No RabbitMQ Stream configured for listenning!")
    rmq_user = os.environ.get("RMQ_USER")
    if not rmq_user:
        raise ValueError("No RabbitMQ user configured!")
    rmq_pw = os.environ.get("RMQ_PASSWORD")
    if not rmq_pw:
        raise ValueError("No RabbitMQ password configured!")
    rmq = RabbitMQConfig(rmq_host, rmq_port, rmq_vhost, rmq_stream, rmq_user, rmq_pw)
    return Config(rmq, sc)


async def main():
    cfg = get_config()

    loop = asyncio.get_event_loop()

    def stop_everything(sig):
        mlzlog.log.info("stopping due to %s", sig)
        for task in asyncio.all_tasks(loop):
            task.cancel()
    loop.add_signal_handler(signal.SIGINT, stop_everything, "SIGINT")
    loop.add_signal_handler(signal.SIGTERM, stop_everything, "SIGTERM")

    i = Ingestor(cfg, mlzlog.getLogger("ingestor"))
    try:
        await i.run()
    except asyncio.CancelledError:
        mlzlog.log.info("stopped")


if __name__ == "__main__":
    mlzlog.initLogging("ingest", logdir=None)  # only to console
    asyncio.run(main())
