#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import pprint
import signal
import sys
from collections import defaultdict
from datetime import datetime
from dataclasses import asdict

import requests
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

from rstream import AMQPMessage, Consumer, ConsumerOffsetSpecification, \
    MessageContext, OffsetType, amqp_decoder, OnClosedErrorInfo
import datablock
import dataset

logging.basicConfig(level=logging.INFO)


class Ingestor:
    def __init__(self, url):
        self.url = url
        self.pid = None
        self.log = logging.getLogger('Ingestor')
        self.log.level= logging.DEBUG
        self.consumer = None

    def login(self, user="ingestor", passwd="aman"):
        r = requests.post(
            self.url + "api/v3/Users/login",
            json={"username": user, "password": passwd}, timeout=100)

        if r.status_code>=400:
           self.log.warn(r.text)
        self.token = r.json()['access_token']

    def post(self, path, **kwds):
        r=  requests.post(f'{self.url}{path}',timeout=100, headers={"Authorization": f"Bearer {self.token}"}, **kwds)
        if  400 <= r.status_code < 500 :
           # try relogin
           self.login(os.environ.get('SCICAT_USER'), os.environ.get('SCICAT_PASSWORD'))
           r=  requests.post(f'{self.url}{path}', headers={"Authorization": f"Bearer {self.token}"}, **kwds)
        if r.status_code > 399:
            self.log.warning(pprint.pformat(r))
            self.log.debug(pprint.pformat(kwds))
        return r



    def createDataset(self, msg):
        md = msg['metadata']
        mapping = msg['mapping']
        exp = mapping['experiment']
        fileinfo = msg.get('fileinfo', {})
        proposal = md[exp]['proposal']
        counterinfo = msg.get('counterinfo',{})
        scaninfo = msg.get('scaninfo',{})
        statistics = msg.get('statistics',{})
        envdata = msg.get('envdata', {})

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


        now = datetime.now()
        np3y = now + relativedelta(years=3)
        np10y = now + relativedelta(years=10)

        d = dataset.Dataset()
        d.fields.id=msg['id']
        d.fields.datasetName = md[exp]['title']
        if scaninfo:
            d.fields.description = scaninfo[d.fields.id]
        else:
            d.fields.description="not available"
        #d.fields.datasetName = msg['metadata']['Exp']['title']
        d.fields.creationLocation = 'MLZ'
        d.fields.owner = "ingestor"
        d.fields.principalInvestigator = md[exp]['users']
        d.fields.contactEmail = "datasteward@frm2.tum.de"
        #d.fields.contactEmail=msg['metadata']['Exp']['localcontact']
        d.fields.ownerEmail = "datasteward@frm2.tum.de"
        if fileinfo:
            d.fields.sourceFolder = fileinfo.get('basepath', '/')
        else:
            d.fields.sourceFolder='/'

        d.fields.creationTime = now.isoformat()
        d.fields.endTime = now.isoformat()
        d.fields.type = "raw"
        d.fields.ownerGroup = proposal
        d.fields.proposalId = proposal
        d.fields.ownerGroup=msg['metadata']['Exp']['proposal'] or 'scicat'
        d.fields.accessGroups = ["mlz", "resi"]
        d.fields.datasetlifecycle.dateOfDiskPurging = np10y.isoformat()
        d.fields.datasetlifecycle.archiveRetentionTime = np10y.isoformat()
        d.fields.datasetlifecycle.dateOfPublishing = np3y.isoformat()
        d.fields.isPublished = False
        sdm = {}
        for f, data in envdata.items():
            if  data.get('std') == 0:
                sdm[f'{f}'] = {'value':data.get('avg','-'), 'unit': data.get('unit','')}
            else:
                sdm[f'{f}_avg'] = {'value':data.get('avg','-'), 'unit': data.get('unit','')}
                sdm[f'{f}_stddev'] = {'value':data.get('std','-'), 'unit': data.get('unit','')}
                sdm[f'{f}_min'] = {'value':data.get('min','-'), 'unit': data.get('unit','')}
                sdm[f'{f}_max'] = {'value':data.get('max','-'), 'unit': data.get('unit','')}

        for f, fv in md.items():
            if f in envdata: continue
            val = fv.get('value')
            if val:
                if isinstance(val, list):
                    if  ',' in val[1]:
                        sdm[f] = {'value': f'{val[0]} {val[1]}', 'unit':''}
                    elif isinstance(val[0], (int,float)):
                        sdm[f] = {'value': val[0], 'unit': val[1]}
                    else:
                         # handle mixed type devices that return a str for some settings
                         sdm[f] = {'value': str(val[0]), 'unit': ''}
                elif isinstance(val, str):
                    sdm[f] = {'value': val, 'unit': ''}

        sdm['runNumber'] = {'value': counterinfo.get('global',-1), 'unit': ''}

        d.fields.scientificMetadata = sdm
        self.log.debug(pprint.pformat(sdm))

        j = d._toJson()

        r = self.post(
            'api/v3/Datasets',
            json=j
        )
        self.log.debug(pprint.pformat(r.text))
        self.pid = r.json().get('pid')

    def addDataBlock(self, flist):
        self.log.info(flist)
        b = datablock.OrigDatablock()
        totalsize=0
        for f in flist.get('filenames',[]):
            fi = datablock.Datafile(f, datetime.now().isoformat())
            fi.size = int(flist['fileinfo'].get(f,{}).get('size',0))
            totalsize += fi.size

            b.dataFileList.append(fi)

        b.size = totalsize
        j = asdict(b)
        #self.log.debug(j)

        upid = requests.utils.quote(self.pid, safe='')

        r = self.post(
            f'api/v3/Datasets/{upid}/origdatablocks/', json=j
        )
        self.log.debug(pprint.pformat(r.text))

    async def consume(self):
        properties: dict[str, str] = defaultdict(str)
        properties["name"] = "consumer-group-2"
        rmq_mode=  OffsetType.FIRST if  os.environ.get('RMQ_MODE', 'last') =='all' else OffsetType.NEXT
        offsetspec = ConsumerOffsetSpecification(rmq_mode, None)
        stream = os.environ.get('RMQ_STREAM')
        self.log.debug(stream)

        try:
            await self.consumer.start()
        except Excpetion:
            sys.exit(-1)
        await self.consumer.subscribe(
            stream=stream, callback=self.on_message, decoder=amqp_decoder,
            properties=properties, offset_specification=offsetspec
        )

        await self.consumer.run()

    async def on_message(
        self, msg_raw: AMQPMessage, message_context: MessageContext
    ):
        msg = json.loads(format(msg_raw))
        self.log.debug("msg[:128]: %s", format(msg_raw)[:128])
        if self.pid != msg.get('id', None) and msg.get('event') == 'scan.end' or msg.get('type') == 'scan.end':
            #self.log.debug(pprint.pformat(msg))
            self.createDataset(msg)
            fp = msg.get('fileinfo')
            if fp:
                self.addDataBlock(fp)
    async def on_connection_closed(disconnection_info: OnClosedErrorInfo) -> None:
        self.log.warning(
            "connection has been closed from stream: "
            + str(disconnection_info.streams)
            + " for reason: "
            + str(disconnection_info.reason)
        )
        await asyncio.sleep(2)
        # reconnect just if the partition exists
        for stream in on_closed_info.streams:
            backoff = 1
            while True:
                try:
                    print("reconnecting stream: {}".format(stream))
                    if consumer is not None:
                        await consumer.reconnect_stream(stream)
                    break
                except Exception as ex:
                    if backoff > 32:
                        # failed to found the leader
                        print("reconnection failed")
                        sys.exit(-1)
                    backoff = backoff * 2
                    print("exception reconnecting waiting 120s: " + str(ex))
                    await asyncio.sleep(30)
                    continue

    async def run_consumer(self):

        self.consumer = Consumer(
            host=os.environ.get('RMQ_HOST', 'localhost'),
            port=int(os.environ.get('RMQ_PORT', '5552')),
            vhost=os.environ.get('RMQ_VHOST', '/'),
            username=os.environ.get('RMQ_USER', 'guest'),
            password=os.environ.get('RMQ_PASSWORD', 'guest'),
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
    i = Ingestor(os.environ.get('SCICAT_URL'))
    if os.environ.get('SCICAT_TOKEN'):
        i.token = os.environ.get('SCICAT_TOKEN')
    else:
        i.login(os.environ.get('SCICAT_USER'), os.environ.get('SCICAT_PASSWORD'))
    i.run()


if __name__ == '__main__':
    run()
