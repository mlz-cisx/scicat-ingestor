import asyncio
from datetime import datetime
from pathlib import Path

import mlzlog

from .client import PostDataset, PostOrigDataBlock, PostProposal, SciCatClient, DataFile, export
from .message import Event, Message, parse_message, testmsg
from .consumer import Backend


class Ingestor(Backend):
    def __init__(self, cfg, log):
        self.log = log
        self.client = SciCatClient(cfg, mlzlog.getLogger("client"))

    async def on_message(self, msg_raw, message_context):
        await self.client.login(self.client.config.user, self.client.config.pw)  # TODO in client
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
            contactEmail="datasteward@mlz-garching.de",  # TODO
            creationLocation="MLZ",  # TODO: instrument?
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

        # proposal_id = await self.client.search_or_upload_proposal(proposal)
        # await self.client.post("api/v3/datasets", json=export(dataset))
        print(export(dataset))

    async def close(self):
        await self.client.close()
