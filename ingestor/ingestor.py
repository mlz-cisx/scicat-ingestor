import math
from datetime import datetime
from pathlib import Path

import mlzlog

from .client import (CreateDatasetDto, CreateOrigDatablockDto,
                     CreateProposalDto, CreateSampleDto, DataFile,
                     SciCatClient, UpdateOrigDataBlockDto, UpdateDatasetDto)
from .consumer import Backend
from .message import Event, Message, parse_message


class Ingestor(Backend):
    def __init__(self, cfg, log):
        self.log = log
        self.client = SciCatClient(cfg, mlzlog.getLogger("client"))

    async def on_message(self, msg_raw, message_context):
        try:
            await self._on_message(msg_raw, message_context)
        except Exception as e:
            self.log.error("Uncaught error in on_message: %r", e)

    async def _on_message(self, msg_raw, message_context):
        self.log.info("processing message:")
        msg: Message = parse_message(msg_raw, self.log)
        self.log.info("message id is %s of type %s", msg.id, msg.event)
        # TODO: change msg w/ hierarchy

        dataset, datablock, proposal, files, sample = self.objects_from_message(msg)
        self._calc_checksums(files)
        await self._upload(dataset, datablock, proposal, files, sample)
        self.log.info("done with message %s", msg.id)

    def objects_from_message(self, msg):
        if msg.event == Event.SCAN or msg.event == Event.ENDSCAN:
            scanid = msg.id
        else:
            scanid = msg.scanid
        if msg.fileinfos:
            sourceFolder = msg.fileinfos[0].proposalpath
        else:
            sourceFolder = "FIXME"
        dataset = CreateDatasetDto(
            type="raw",
            owner="ingestor",
            ownerGroup=msg.propinfo.session,
            accessGroups=[msg.propinfo.session, msg.propinfo.instrument],
            sourceFolder=sourceFolder,  # TODO: sometimes there is no file yet, and we can't know
            contactEmail="datasteward@mlz-garching.de",  # TODO
            creationLocation="MLZ",  # TODO: instrument?
            principalInvestigator=msg.propinfo.mainproposer.name,
            datasetName=msg.name or "Scan",
            creationTime=datetime.now().isoformat(),
            pid=scanid,
            isPublished=False,
            description="not available",
            scientificMetadata=self.get_scientific_metadata(msg),
            runNumber=str(0),
            sampleId=str(msg.sample.id),
            instrumentId=msg.instrument,
        )
        proposal = CreateProposalDto(
            ownerGroup=msg.propinfo.session,
            accessGroups=[msg.propinfo.instrument],
            proposalId=msg.propinfo.proposal + "test",
            email=msg.propinfo.mainproposer.email,
            title=msg.propinfo.title,
        )

        sample = CreateSampleDto(
            sampleId=str(msg.sample.id),
            owner="ingestor",
            ownerGroup="ingestor",
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
        return dataset, datablock, proposal, files, sample

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

    async def _upload(self, dataset, datablock, proposal, files, sample):
        # create proposal if not present
        prop_from_scicat = await self._get_or_create_proposal(proposal)
        dataset.proposalId = str(prop_from_scicat.proposalId)
        # ensure sample exists
        if await self.client.get_sample_by_id(sample.sampleId, not_found_ok=True) is None:
            self.log.info("Creating sample %s in SciCat", sample.sampleId)
            await self.client.create_sample(sample)
        else:
            self.log.info("Sample %s found", sample.sampleId)
        instrument = await self.client.instrument_by_unique_name(f"MLZ.{dataset.instrumentId}")
        if instrument:
            dataset.instrumentId = instrument.pid
        else:
            self.log.warning("Unknown instrument %r, submitting it anyway.",
                             dataset.instrumentId)

        # create dataset if not present
        if await self.client.get_dataset_by_pid(dataset.pid, not_found_ok=True) is None:
            self.log.info("Creating Dataset %s in SciCat", dataset.pid)
            await self.client.create_dataset(dataset)
            await self._create_datablock(dataset, datablock)
        else:
            # modify existing
            await self._add_files_to_dataset(dataset, datablock, files)

        # Workaround for base filepath missing
        ds = await self.client.get_dataset_by_pid(dataset.pid, not_found_ok=True)
        if ds.sourceFolder == "FIXME" and ds and dataset.sourceFolder != "FIXME":
            ds_patch = UpdateDatasetDto(sourceFolder=dataset.sourceFolder)
            await self.client.modify_dataset(ds.pid, ds_patch)

    async def _get_or_create_proposal(self, proposal):
        present = await self.client.get_proposal_by_pid(proposal.proposalId, not_found_ok=True)
        if present is not None:
            self.log.info("Proposal %s found", proposal.proposalId)
            return present
        self.log.info("Proposal %s not found in SciCat, creating.", proposal.proposalId)
        return await self.client.create_proposal(proposal)

    async def _create_datablock(self, dataset, datablock):
        if datablock.dataFileList:
            # datablock without files is an error, so we have to do it later
            self.log.info("Creating Datablock for dataset %s", dataset.pid)
            return await self.client.create_origdatablock(datablock)
        return None

    async def _add_files_to_dataset(self, dataset, datablock, files):
        self.log.info("Dataset %s already exists, adding files", dataset.pid)
        datablock_from_scicat = await self.client.get_origdatablocks_for_dataset(dataset.pid)
        if datablock_from_scicat:
            datablock_from_scicat = datablock_from_scicat[-1]
        else:
            datablock_from_scicat = await self._create_datablock(dataset, datablock)
        if datablock_from_scicat is None or not datablock.dataFileList:
            self.log.info("No new files to add.")
            return
        # patch with new files
        prev = {f.path: f for f in datablock_from_scicat.dataFileList}
        curr = {f.path: f for f in files}
        newfiles = []
        newsize = 0
        new = 0
        todos = set(f.path for f in files).union(set(prev.keys()))
        for file in todos:
            if file in curr and file in prev:
                newfiles.append(curr[file])
                newsize += curr[file].size
            elif file in curr:
                newfiles.append(curr[file])
                newsize += curr[file].size
                new += 1
            elif file in prev:
                newfiles.append(prev[file])
                newsize += prev[file].size
        update = UpdateOrigDataBlockDto(size=newsize, dataFileList=newfiles)
        await self.client.update_datablock(datablock_from_scicat._id, update)
        self.log.info("Added %d new files (%db)", new, newsize - datablock.size)

    def _calc_checksums(self, files):
        pass

    async def close(self):
        await self.client.close()
