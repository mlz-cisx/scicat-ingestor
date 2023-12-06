#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import pprint
import signal
from collections import defaultdict
from datetime import datetime

import requests
from dateutil.relativedelta import relativedelta

import datablock
import dataset

logging.basicConfig(level=logging.DEBUG)
from rstream import AMQPMessage, Consumer, ConsumerOffsetSpecification, \
    MessageContext, OffsetType, amqp_decoder

STREAM = "xresd"


class Ingestor:
    def __init__(self, url):
        self.url = url
        self.pid = None
        self.log = logging.getLogger('Ingestor')

    def login(self, user="ingestor", passwd="aman"):
        r = requests.post(
            self.url + "api/v3/Users/login",
            json={"username": user, "password": passwd}
        )
        self.log.debug(r.text)
        self.token = r.json()['access_token']

    def createDataset(self, msg):
        md = msg['metadata']
        mapping = msg['mapping']
        exp = mapping['experiment']
        now = datetime.now()
        np3y = now + relativedelta(years=3)

        d = dataset.Dataset()

        d.fields.datasetName = md[exp]['title']
        #d.fields.datasetName = msg['metadata']['Exp']['title']
        d.fields.creationLocation = 'MLZ'
        d.fields.owner = "ingestor"
        d.fields.principalInvestigator = md[exp]['users']
        d.fields.contactEmail = "bjoern.pedersen@frm2.tum.de"
        #d.fields.contactEmail=msg['metadata']['Exp']['localcontact']
        d.fields.ownerEmail = "bjoern.pedersen@frm2.tum.de"
        d.fields.sourceFolder = "/some/folder"
        d.fields.creationTime = now.isoformat()
        d.fields.endTime = now.isoformat()
        d.fields.type = "raw"
        d.fields.ownerGroup = md[exp]['proposal']
        #d.fields.ownerGroup=msg['metadata']['Exp']['proposal']
        d.fields.accessGroups = ["mlz", "resi"]
        d.fields.datasetlifecycle.dateOfDiskPurging = np3y.isoformat()
        d.fields.datasetlifecycle.archiveRetentionTime = np3y.isoformat()
        d.fields.datasetlifecycle.dateOfPublishing = np3y.isoformat()
        d.fields.datasetlifecycle.publishedOn = np3y.isoformat()
        d.fields.isPublished = False
        sdm = {}
        for f, fv in md.items():
            val = fv.get('value')
            if val:
                if isinstance(val, list):
                    sdm[f] = {'value': val[0], 'unit': val[1]}
                elif isinstance(val, str):
                    sdm[f] = {'value': val, 'unit': ''}

        d.fields.scientificMetadata = sdm

        j = d._toJson()

        r = requests.post(
            f'{self.url}api/v3/Datasets',
            headers={"Authorization": f"Bearer {self.token}"}, json=j
        )
        self.log.debug(r.text)
        self.pid = r.json().get('pid')

    def addDataBlock(self, flist):
        self.log.info(flist)
        b = datablock.origdatablock()
        for f in flist:
            fi = datablock.datafile(f, datetime.now().isoformat())
            fi.size = 200

            b.dataFileList.append(fi)

        b.size = len(b.dataFileList)
        j = datablock.asdict(b)
        self.log.debug(j)

        upid = requests.utils.quote(self.pid, safe='')

        r = requests.post(
            f'{self.url}api/v3/Datasets/{upid}/origdatablocks/', json=j,
            headers={"Authorization": f"Bearer {self.token}"}
        )
        self.log.debug(r.text)

    async def consume(self):
        properties: dict[str, str] = defaultdict(str)
        properties["name"] = "consumer-group-2"
        rmq_mode=  OffsetType.FIRST if  os.environ.get('RMQ_MODE', 'last') =='all' else OffsetType.NEXT
        offsetspec = ConsumerOffsetSpecification(rmq_mode, None)
        stream = os.environ.get('RMQ_STREAM')
        self.log.debug(stream)

        await self.consumer.start()
        await self.consumer.subscribe(
            stream=stream, callback=self.on_message, decoder=amqp_decoder,
            properties=properties, offset_specification=offsetspec
        )

        await self.consumer.run()

    async def on_message(
        self, msg_raw: AMQPMessage, message_context: MessageContext
    ):
        msg = json.loads(format(msg_raw))
        self.log.debug(pprint.pformat(msg))
        if self.pid != msg['id'] and msg['type'] == 'scan.end':
            self.createDataset(msg)
            fp = msg.get('filepaths')
            if fp:
                self.addDataBlock(fp)

    async def run_consumer(self):
        self.consumer = Consumer(
            host=os.environ.get('RMQ_HOST', 'localhost'),
            port=int(os.environ.get('RMQ_PORT', '5552')),
            vhost=os.environ.get('RMQ_VHOST', '/'),
            username=os.environ.get('RMQ_USER', 'guest'),
            password=os.environ.get('RMQ_PASSWORD', 'guest'),
        )
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(
            signal.SIGINT, lambda: asyncio.create_task(self.consumer.close())
        )

        await self.consume()

    def run(self):
        asyncio.run(self.run_consumer(), debug=False)


def run():
    i = Ingestor(os.environ.get('SCICAT_URL'))
    if os.environ.get('SCICAT_TOKEN'):
        i.token = os.environ.get('SCICAT_TOKEN')
    else:
        i.login(os.environ.get('SCICAT_USER'), os.environ.get('SCICAT_PASSWORD'))
    i.run()


if __name__ == '__main__':
    run()
