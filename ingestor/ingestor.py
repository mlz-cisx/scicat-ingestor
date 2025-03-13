import asyncio
from datetime import datetime
import math
from pathlib import Path

import mlzlog

from .client import (CreateDatasetDto, CreateOrigDatablockDto, CreateSampleDto,
                     CreateProposalDto, DataFile, SciCatClient)
from .consumer import Backend
from .message import Event, Message, parse_message


class Ingestor(Backend):
    def __init__(self, cfg, log):
        self.log = log
        self.client = SciCatClient(cfg, mlzlog.getLogger("client"))

    async def on_message(self, msg_raw, message_context):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._on_message, msg_raw, message_context)

    def _on_message(self, msg_raw, message_context):
        self.client.login(self.client.config.user, self.client.config.pw)  # TODO in client
        msg: Message = parse_message(msg_raw, self.log)
        # TODO: change msg w/ hierarchy
        if msg.event == Event.SCAN or msg.event == Event.ENDSCAN:
            scanid = msg.id
        else:
            scanid = msg.scanid

        dataset = CreateDatasetDto(
            type="raw",
            owner="ingestor",
            ownerGroup=msg.propinfo.session,
            accessGroups=[msg.propinfo.session, msg.propinfo.instrument],
            sourceFolder="/",  # TODO: sometimes there is no file yet, and we can't know
            contactEmail="datasteward@mlz-garching.de",  # TODO
            creationLocation="MLZ",  # TODO: instrument?
            principalInvestigator=msg.propinfo.mainproposer.name,
            datasetName=msg.name,
            creationTime=datetime.now().isoformat(),
            pid=scanid,
            isPublished=True,
            description="not available",
            scientificMetadata=self.get_scientific_metadata(msg),
            runNumber=str(0),
            sample_id=str(msg.sample.id),
            instrumentId="",
        )
        proposal = CreateProposalDto(
            ownerGroup=msg.propinfo.session,
            accessGroups=[msg.propinfo.instrument],
            proposalId=msg.propinfo.proposal,
            email="",
            title=msg.propinfo.title,
        )

        sample = CreateSampleDto(
            sample_id=str(msg.sample.id),
            owner="",
            ownerGroup="",
            accessGroups=[],
            instrumentGroup="",
            description=msg.sample.name,
        )

        files = []
        totalsize = 0
        for fileinfo in msg.fileinfos:
            path = Path(fileinfo.path)
            samplepath = Path(fileinfo.samplepath)
            path = str(path.relative_to(samplepath))
            totalsize += fileinfo.size
            files.append(DataFile(
                path=path,
                size=fileinfo.size,
                time=fileinfo.ctime,
                chk=None,  # TODO
                uid=str(fileinfo.uid),
                gid=str(fileinfo.gid),
                perm=str(fileinfo.mode),
            ))

        datablock = CreateOrigDatablockDto(
            datasetId=dataset.pid,
            ownerGroup=msg.propinfo.session,
            accessGroups=[msg.propinfo.instrument],
            instrumentGroup=msg.propinfo.instrument,
            # chkAlg=,
            dataFileList=files,
            size=totalsize,
        )

        self._calc_checksums(files)
        self._upload(dataset, datablock, proposal, files, sample)

    def get_scientific_metadata(self, msg):
        smd = {}
        for dev, stats in msg.statistics.items():
            unit = msg.metadata.get(dev, {}).get("value", {}).get("unit", "")
            unit = self._map_unit(unit)
            for key, value in stats._asdict().items():
                if key == 'stddev' and (stats.stddev == 0.0 or math.isinf(stats.stddev)):
                    v = 0.0
                else:
                    v = value
                smd[f"{dev}_{key}"] = {
                    "value": v,
                    "unit": unit,
                }
        for dev, dev_params in msg.metadata.items():
            if dev in msg.statistics:
                continue
            for param, parammd in dev_params.items():
                if not parammd:
                    continue
                val = parammd.get("value")
                if val is None:
                    continue
                unit = self._map_unit(parammd.get("unit", ""))
                cat_name = f"{dev}_{param}" if param != "value" else dev
                if isinstance(val, list):
                    if len(val) > 1 and isinstance(val[1], str) and "," in val[1]:
                        smd[cat_name] = {"value": f"{val[0]} {val[1]}", "unit": unit}
                    elif isinstance(val[0], (int, float)):
                        smd[cat_name] = {"value": val[0], "unit": unit}
                    else:
                        # handle mixed type devices that return a str for some settings
                        smd[cat_name] = {"value": str(val[0]), "unit":  ''}
                elif isinstance(val, str):
                    smd[cat_name] = {"value": val, "unit": ""}
                else:
                    smd[cat_name] = {"value": val, "unit": unit}
        smd["runNumber"] = {"value": msg.counterinfo.counter, "unit": ""}
        return smd

    def _map_unit(self, unit):
        return unit.replace("A-1", "angstrom^-1") #.replace("K", "kelvin")

    def _upload(self, dataset, datablock, proposal, files, sample):
        # create proposal if not present
        prop_from_scicat = self.client.get_or_create_proposal(proposal)
        dataset.proposalId = str(prop_from_scicat)
        # ensure sample exists
        if self.client.get_sample_by_id(sample.sample_id) is None:
            self.client.create_sample(sample)

        # create dataset if not present
        if self.client.get_dataset_by_pid(dataset.pid) is None:
            self.client.create_dataset(dataset)
        #
        datablocks = self.client.get_origdatablocks_for_dataset(dataset.pid)
        if not datablocks:
            datablock_scicat = self.client.create_origdatablock(datablock)
        else:
            datablock_scicat = datablocks[-1]
        datablock_scicat.pid
        # patch with new files

    def _calc_checksums(self, files):
        pass

    async def close(self):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._close)

    def _close(self):
        self.client.close()
