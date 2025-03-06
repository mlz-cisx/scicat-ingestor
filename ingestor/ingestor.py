import asyncio
from datetime import datetime
from pathlib import Path

import mlzlog
import rstream

from .client import PostDataset, PostOrigDataBlock, PostProposal, SciCatClient, DataFile, export
from .message import Event, Message, parse_message, testmsg


class Ingestor:
    def __init__(self, cfg, log):
        self.cfg = cfg
        self.client = SciCatClient(cfg.sc, mlzlog.getLogger("client"))
        self.log = log
        self.consumer = None

    async def on_message(self, msg_raw, message_context):
        msg: Message = parse_message(msg_raw, self.log)
        if msg.event == Event.SCAN or msg.event == Event.ENDSCAN:
            scanid = msg.id
        else:
            scanid = msg.scanid

        dataset = PostDataset(
            type="raw",
            owner="ingestor",
            ownerGroup=msg.propinfo.session,
            accessGroups=[msg.propinfo.session, msg.propinfo.instrument],
            sourceFolder="/",  # TODO: sometimes there is no file yet, and we can't know
            contactEmail="a@b.com",
            creationLocation="",
            principalInvestigator="",
            datasetName=msg.name,
            creationTime=datetime.now().isoformat(),
            pid=scanid,
            isPublished=True,
        )
        proposal = PostProposal(
            ownerGroup=msg.propinfo.session,
            proposalId="",
            email="",
            title="",
        )

        files = []
        for fileinfo in msg.fileinfos:
            path = Path(fileinfo.path)
            samplepath = Path(fileinfo.samplepath)
            path = str(path.relative_to(samplepath))
            files.append(DataFile(
                path=path,
                size=fileinfo.size,
                time=fileinfo.ctime,
                chk=None,  # TODO
                uid=fileinfo.uid,
                gid=fileinfo.gid,
                perm=fileinfo.mode,
            ))

        proposal_id = await self.client.search_or_upload(proposal)
        #await self.client.post("api/v3/datasets", json=export(dataset))
        print(export(dataset))

    async def run(self):
        await self.client.login(self.client.config.user, self.client.config.pw)  # TODO in client
        for msg in testmsg():
            await self.on_message(msg, None)
        try:
            pass
            # r = self.cfg.rmq
            # print(r.host, r.port, r.vhost, r.user, r.pw)
            # self.consumer = rstream.Consumer(
            #     host=r.host,
            #     port=r.port,
            #     vhost=r.vhost,
            #     username=r.user,
            #     password=r.pw,
            #     # on_close_handler=,
            #     load_balancer_mode=True,  # TODO: does not work without, what it do?
            # )
            # try:
            #     await self.consumer.start()
            # except Exception as e:
            #     self.log.error("Connecting to RabbitMQ failed: %r", e)
            # await self.consumer.subscribe(
            #     stream=r.stream,
            #     subscriber_name="scicat-ingestor",  # TODO: from cfg or static?
            #     callback=self.on_message,
            #     decoder=rstream.amqp_decoder,
            #     offset_specification=rstream.ConsumerOffsetSpecification(rstream.OffsetType.NEXT, None),  # TODO: offset tracking
            # )
            # await self.consumer.run()
        finally:
            if self.consumer:
                await self.consumer.close()
            await self.client.close()
