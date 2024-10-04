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

import requests
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from rstream import AMQPMessage, Consumer, ConsumerOffsetSpecification, \
    MessageContext, OffsetType, OnClosedErrorInfo, amqp_decoder

import datablock
import dataProposal
import dataset

logging.basicConfig(level=logging.INFO)


class Ingestor:
    def __init__(self, url):
        self.url = url
        self.pid = None
        self.log = logging.getLogger("Ingestor")
        self.log.level = logging.DEBUG
        self.consumer = None
        self.token = None
        self.proposaltoken = None

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
            self.url + "api/v3/Users/login",
            json={"username": user, "password": passwd},
            timeout=100,
        )

        if res.status_code >= 400:
            self.log.warning(res.text)
        self.token = res.json()["access_token"]

        res = requests.post(
            self.url + "api/v3/Users/login",
            json={"username": "proposalIngestor", "password": passwd},
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

        try:
            return res.json()
        except Exception:
            self.log.exception("error get proposal")
            return None

    def createDataset(self, msg):
        md = msg["metadata"]
        mapping = msg["mapping"]
        exp = mapping["experiment"]
        fileinfo = msg.get("fileinfo", {})
        proposal = md[exp]["proposal"]["value"]
        counterinfo = msg.get("counterinfo", {})
        scaninfo = msg.get("scaninfo", {})
        statistics = msg.get("statistics", {})
        envdata = msg.get("envdata", {})
        instrument = mapping["instrument"].upper()

        # instrumentuid = self.get_instrument_by_pid(instrument)

        self.log.debug("===== mapping =====")
        self.log.debug(pprint.pformat(mapping))
        self.log.debug("===== metadata =====")
        self.log.debug(pprint.pformat(md))
        self.log.debug("===== fileinfo =====")
        self.log.debug(pprint.pformat(fileinfo))
        self.log.debug("===== proposal =====")
        self.log.debug(pprint.pformat(proposal))
        self.log.debug("===== counterinfo =====")
        self.log.debug(pprint.pformat(counterinfo))
        self.log.debug("===== scaninfo =====")
        self.log.debug(pprint.pformat(scaninfo))
        self.log.debug("===== statistics =====")
        self.log.debug(pprint.pformat(statistics))
        self.log.debug("===== envdata =====")
        self.log.debug(pprint.pformat(envdata))
        self.log.debug(pprint.pformat(instrument))

        now = datetime.now()
        np3y = now + relativedelta(years=3)
        np10y = now + relativedelta(years=10)

        d = dataset.Dataset()
        d.fields.id = msg["id"]
        d.fields.datasetName = md[exp]["title"]["value"]
        if scaninfo:
            d.fields.description = scaninfo[d.fields.id]
        else:

            d.fields.description = "not available"
        # d.fields.datasetName = msg['metadata']['Exp']['title']
        d.fields.creationLocation = "MLZ"
        d.fields.owner = "ingestor"
        d.fields.principalInvestigator = md[exp]["users"]["value"]
        d.fields.contactEmail = "datasteward@frm2.tum.de"
        d.fields.ownerEmail = "datasteward@frm2.tum.de"
        if fileinfo:
            d.fields.sourceFolder = fileinfo.get("basepath", "/")
        else:
            d.fields.sourceFolder = "/"

        d.fields.creationTime = now.isoformat()
        d.fields.endTime = now.isoformat()
        d.fields.type = "raw"
        d.fields.ownerGroup = proposal
        d.fields.proposalId = proposal
        d.fields.accessGroups = ["mlz", "resi"]
        d.fields.datasetlifecycle.dateOfDiskPurging = np10y.isoformat()
        d.fields.datasetlifecycle.archiveRetentionTime = np10y.isoformat()
        d.fields.datasetlifecycle.dateOfPublishing = np3y.isoformat()
        d.fields.isPublished = False
        d.fields.ownerGroup = proposal or "scicat"
        proposaluid = self.get_proposal_by_pid(proposal)
        self.log.debug("get_proposal: %r", proposaluid)
        if proposaluid is not None:
            d.fields.proposalId = proposaluid["_id"]
        else:
            p = dataProposal.ProposalDataset()
            p.fields.proposalId = proposal
            p.fields.email = "email@tum.com"
            p.fields.title = "title"
            p.fields.ownerGroup = "ingestor"

            j = p._toJson()
            self.log.debug(pprint.pformat(j))
            pr = self.postProposal("api/v3/proposals", json=j)

            self.log.debug(pprint.pformat(pr.text))
            propinfo = pr.json()
            d.fields.proposalId = propinfo["id"]

        instrumentuid = self.get_instrument_by_pid(instrument)
        if instrumentuid is not None:
            d.fields.instrumentId = instrumentuid['id']
        # else:
        #    inst = dataInstrument.InstrumentDataset()
        #    inst.fields.instrumentId = instrument
        #    inst.fields.name = "InstName"

        #    j = inst._toJson()
        #    self.log.debug(pprint.pformat(j))
        #    r = self.postInstrument(
        #         'api/v3/instruments',
        #         json=j
        #    )

        sdm = {}
        for f, data in envdata.items():
            data["unit"] = data.get("unit", "").replace("A-1", "angstrom^-1")
            if data.get("std") == 0:
                sdm[f"{f}"] = {
                    "value": data.get("avg", "-"),
                    "unit": data.get("unit", ""),
                }
            else:
                sdm[f"{f}_avg"] = {
                    "value": data.get("avg", "-"),
                    "unit": data.get("unit", ""),
                }
                sdm[f"{f}_stddev"] = {
                    "value": data.get("std", "-"),
                    "unit": data.get("unit", ""),
                }
                sdm[f"{f}_min"] = {
                    "value": data.get("min", "-"),
                    "unit": data.get("unit", ""),
                }
                sdm[f"{f}_max"] = {
                    "value": data.get("max", "-"),
                    "unit": data.get("unit", ""),
                }

        for f, fv in md.items():

            if f in envdata:
                continue
            val = fv.get("value")
            if val:
                if isinstance(val, list):
                    if "," in val[1]:
                        sdm[f] = {"value": f"{val[0]} {val[1]}", "unit": ""}
                    elif isinstance(val[0], (int, float)):
                        sdm[f] = {
                            "value": val[0],
                            "unit": val[1].replace("A-1", "angstrom^-1"),
                        }
                    else:
                        # handle mixed type devices that return a str for some settings
                        sdm[f] = {"value": str(val[0]), "unit": ""}
                elif isinstance(val, str):
                    sdm[f] = {"value": val, "unit": ""}

        sdm["runNumber"] = {"value": counterinfo.get("global", -1), "unit": ""}

        d.fields.scientificMetadata = sdm
        self.log.debug(pprint.pformat(sdm))

        j = d._toJson()

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
            msg = json.loads(str(msg_raw._body))
        except Exception as e:
            self.log.debug(e)
            self.log.debug(format(msg_raw)[:128])
        self.log.debug("msg[:128]: %s", format(msg_raw)[:128])
        if (msg.get('version') == 'v1alpha1' and
            self.pid != msg.get("id", None)
            and msg.get("event") == "scan.end"
            or msg.get("type") == "scan.end"
        ):
            # self.log.debug(pprint.pformat(msg))
            self.createDataset(msg)
            fp = msg.get("fileinfo")
            if fp:
                self.addDataBlock(fp)

    async def on_connection_closed(self, disconnection_info: OnClosedErrorInfo) -> None:
        self.log.warning(
            "connection has been closed from stream: %s for reason: %s",disconnection_info.streams, disconnection_info.reason
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
    i = Ingestor(os.environ.get("SCICAT_URL"))
    if os.environ.get("SCICAT_TOKEN"):
        i.token = os.environ.get("SCICAT_TOKEN")
    else:
        i.login(os.environ.get("SCICAT_USER"), os.environ.get("SCICAT_PASSWORD"))

    i.run()


if __name__ == "__main__":
    run()
