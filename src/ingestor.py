#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import pprint
import signal
import sys
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
from enum import StrEnum

import requests
from dotenv import load_dotenv
from rstream import AMQPMessage, Consumer, ConsumerOffsetSpecification, \
    MessageContext, OffsetType, OnClosedErrorInfo, amqp_decoder

import datablock
import dataproposal
from messageparser import MessageV1Alpha1

logging.basicConfig(level=logging.DEBUG)


class AuthStrategy(StrEnum):
    FUNCTIONAL = "login"
    LDAP = "ldap"


class Ingestor:
    def __init__(self, url, *, auth_strategy=None):
        self.url = url
        self.pid = None
        self.log = logging.getLogger("Ingestor")
        self.log.level = logging.DEBUG
        self.consumer = None
        self.token = None
        self.proposaltoken = None
        if auth_strategy:
            try:
                self._auth_strategy = AuthStrategy(auth_strategy)
            except ValueError as e:
                raise ValueError(
                    f"Argument {auth_strategy!r} is not a valid authentication strategy. "
                    f"Use 'login' or 'ldap'"
                ) from e
        else:
            self._auth_strategy = AuthStrategy.FUNCTIONAL

    def login(self, user="ingestor", passwd="aman"):
        """
        Sign in to SciCat and set the access token for this instance.
        Parameters
        ----------
        username : str
          Username of a functional account
        password : str
          Password of a functional account

        Returns
        -------
        None
        """

        res = requests.post(
            f"{self.url}api/v3/auth/{self._auth_strategy}",
            json={"username": user, "password": passwd},
            timeout=100,
        )

        if res.status_code >= 400:
            self.log.warning(res.text)
        self.token = res.json()["access_token"]

        # Authenticate for proposals
        # TODO: is this needed? why is the passwd the same as the normal ingestor?
        # either config two accounts or let one do both
        res = requests.post(
            f"{self.url}api/v3/auth/{self._auth_strategy}",
            json={"username": user, "password": passwd},
            timeout=100,
        )

        if res.status_code >= 400:
            self.log.warning(res.text)
        self.proposaltoken = res.json()["access_token"]

    def post(self, path, **kwds):
        res = requests.post(
            f"{self.url}{path}",
            timeout=100,
            headers={"Authorization": f"Bearer {self.token}"},
            **kwds,
        )
        if 400 <= res.status_code < 500:
            # try relogin
            self.login(os.environ.get("SCICAT_USER"), os.environ.get("SCICAT_PASSWORD"))
            res = requests.post(
                f"{self.url}{path}",
                headers={"Authorization": f"Bearer {self.token}"},
                **kwds,
            )
        if res.status_code > 399:
            self.log.warning(pprint.pformat(res))
            self.log.debug(pprint.pformat(kwds))
        return res

    def get(self, path, **kwds):
        res = requests.get(
            f"{self.url}{path}",
            timeout=100,
            headers={"Authorization": f"Bearer {self.token}"},
            **kwds,
        )
        if 400 <= res.status_code < 500:
            # try relogin
            self.login(os.environ.get("SCICAT_USER"), os.environ.get("SCICAT_PASSWORD"))
            res = requests.get(
                f"{self.url}{path}",
                headers={"Authorization": f"Bearer {self.token}"},
                **kwds,
            )
        if res.status_code > 399:
            self.log.warning(pprint.pformat(res))
            self.log.debug(pprint.pformat(kwds))
        return res

    def postProposal(self, path, **kwds):
        res = requests.post(
            f"{self.url}{path}",
            timeout=100,
            headers={"Authorization": f"Bearer {self.token}"},
            **kwds,
        )
        if 400 <= res.status_code < 500:
            # try relogin
            self.login(os.environ.get("SCICAT_USER"), os.environ.get("SCICAT_PASSWORD"))
            res = requests.post(
                f"{self.url}{path}",
                headers={"Authorization": f"Bearer {self.proposaltoken}"},
                **kwds,
            )
        if res.status_code > 399:
            self.log.warning(pprint.pformat(res))
            self.log.debug(pprint.pformat(kwds))
        return res

    def postInstrument(self, path, **kwds):
        res = requests.post(
            f"{self.url}{path}",
            timeout=100,
            headers={"Authorization": f"Bearer {self.token}"},
            **kwds,
        )
        if 400 <= res.status_code < 500:
            # try relogin
            self.login(os.environ.get("SCICAT_USER"), os.environ.get("SCICAT_PASSWORD"))
            res = requests.post(
                f"{self.url}{path}",
                headers={"Authorization": f"Bearer {self.proposaltoken}"},
                **kwds,
            )
        if res.status_code > 399:
            self.log.warning(pprint.pformat(res))
            self.log.debug(pprint.pformat(kwds))
        return res

    def get_proposal_by_pid(self, pid) -> dict:
        """
            Get proposal by pid.

            Parameters
        ----------
            pid : str
            The pid of the proposal

            Returns
        -------
            dict
            The proposal with the requested pid
        """

        endpoint = f"api/v3/Proposals/{pid}"
        res = self.get(endpoint)

        if res.status_code >= 400:
            self.log.warning(res.text)
            return None

        if not res.text:  # Empty answer with status 200 indicates "not found"?
            return None
        try:
            return res.json()
        except Exception:
            self.log.exception("error get proposal")
            return None

    def get_instrument_by_pid(self, pid) -> dict:
        """
            Get an instrument by pid.

            Parameters
        ----------
            pid : str
            The pid of the instrument

            Returns
        -------
            dict
            The instrument with the requested pid
        """

        endpoint = f"api/v3/Instruments/{pid}"
        res = self.get(endpoint)

        if res.status_code >= 400:
            self.log.warning(res.text)
            return None
        try:
            return res.json()
        except Exception:
            self.log.exception("error get proposal")
            return None

    def createDataset(self, msg):
        data_set = MessageV1Alpha1(self.log).parse_message(
            msg,
            pid_cb=self.get_proposal_by_pid,
            instr_cb=self.get_instrument_by_pid,
        )
        if not data_set.proposalId:
            p = dataproposal.ProposalDataset()
            p.proposalId = data_set.ownerGroup
            p.email = "email@tum.com"
            p.title = "title"
            p.ownerGroup = "ingestor"

            j = p._toJson()
            self.log.debug(pprint.pformat(j))
            pr = self.postProposal("api/v3/proposals", json=j)

            self.log.debug(pprint.pformat(pr.text))
            propinfo = pr.json()
            data_set.proposalId = propinfo["id"]

        j = data_set._toJson()

        r = self.post("api/v3/Datasets", json=j)

        self.log.debug(pprint.pformat(r.text))
        self.pid = r.json().get("pid")

    def addDataBlock(self, flist):
        self.log.info(flist)
        b = datablock.OrigDatablock()
        totalsize = 0
        for f in flist.get("filenames", []):
            fi = datablock.Datafile(f, datetime.now().isoformat())
            fi.size = int(flist["fileinfo"].get(f, {}).get("size", 0))
            totalsize += fi.size

            b.dataFileList.append(fi)

        b.size = totalsize
        j = asdict(b)
        # self.log.debug(j)

        upid = requests.utils.quote(self.pid, safe="")

        res = self.post(f"api/v3/Datasets/{upid}/origdatablocks/", json=j)
        self.log.debug(pprint.pformat(res.text))
        self.log.debug(self.token)

    async def consume(self):
        properties: dict[str, str] = defaultdict(str)
        properties["name"] = "consumer-group-2"
        rmq_mode = (
            OffsetType.FIRST
            if os.environ.get("RMQ_MODE", "last") == "all"
            else OffsetType.NEXT
        )
        offsetspec = ConsumerOffsetSpecification(rmq_mode, None)
        stream = os.environ.get("RMQ_STREAM")
        self.log.debug(stream)

        try:
            await self.consumer.start()
        except Exception as e:
            self.log.error("stream conn failed: %r", e)
            sys.exit(-1)
        await self.consumer.subscribe(
            stream=stream,
            callback=self.on_message,
            decoder=amqp_decoder,
            properties=properties,
            offset_specification=offsetspec,
        )

        await self.consumer.run()

    async def on_message(self, msg_raw: AMQPMessage, _message_context: MessageContext):
        try:
            msg = json.loads(msg_raw.body.decode("utf-8"))
        except Exception as e:
            self.log.debug(e)
            self.log.debug(format(msg_raw)[:128])
        self.log.debug("msg[:128]: %s", format(msg_raw)[:128])
        with open('msgs.log', "a") as f:
            json.dump(msg, f)
            f.write('\n')
        if (
            msg.get("version") == "v1alpha1"
            and self.pid != msg.get("id", None)
            and (msg.get("event") == "scan.end"
                 or msg.get("type") == "scan.end")
        ):
            self.createDataset(msg)
            fp = msg.get("fileinfo")
            if fp:
                self.addDataBlock(fp)

    async def on_connection_closed(self, disconnection_info: OnClosedErrorInfo) -> None:
        self.log.warning(
            "connection has been closed from stream: %s for reason: %s",
            disconnection_info.streams,
            disconnection_info.reason,
        )
        await asyncio.sleep(2)
        # reconnect just if the partition exists
        for stream in disconnection_info.streams:
            backoff = 1
            while True:
                try:
                    self.log.debug("reconnecting stream: %s", stream)
                    if self.consumer is not None:
                        await self.consumer.reconnect_stream(stream)
                    break
                except Exception as ex:
                    if backoff > 32:
                        # failed to found the leader
                        self.log.exception("reconnection failed")
                        sys.exit(-1)
                    backoff = backoff * 2
                    self.log.warning("exception reconnecting waiting 120s: %s ", ex)
                    await asyncio.sleep(30)
                    continue

    async def run_consumer(self):
        self.consumer = Consumer(
            host=os.environ.get("RMQ_HOST", "localhost"),
            port=int(os.environ.get("RMQ_PORT", "5552")),
            vhost=os.environ.get("RMQ_VHOST", "/"),
            username=os.environ.get("RMQ_USER", "guest"),
            password=os.environ.get("RMQ_PASSWORD", "guest"),
            on_close_handler=self.on_connection_closed,
            load_balancer_mode=True,
        )
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(
            signal.SIGINT, lambda: asyncio.create_task(self.consumer.close())
        )
        task = asyncio.create_task(self.consume())
        done, _ = await asyncio.wait([task])

        for task in done:
            if task.exception():
                raise task.exception()
            self.log.info(task.result())

    def run(self):
        asyncio.run(self.run_consumer(), debug=False)


def run():
    load_dotenv()
    i = Ingestor(
        os.environ.get("SCICAT_URL"),
        auth_strategy=os.environ.get("SCICAT_AUTH_STRATEGY"),
    )
    if os.environ.get("SCICAT_TOKEN"):
        i.token = os.environ.get("SCICAT_TOKEN")
    else:
        i.login(os.environ.get("SCICAT_USER"), os.environ.get("SCICAT_PASSWORD"))

    i.run()


if __name__ == "__main__":
    run()
