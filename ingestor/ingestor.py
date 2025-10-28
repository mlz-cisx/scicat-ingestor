import math
import time

import mlzlog

from .client import SciCatClient, UpdateDatasetDto, UpdateOrigDataBlockDto, PrecondFailed
from .consumer import Backend
from .message import Event, Message, parse_message
from .workqueue import RmqClient


class Ingestor(Backend):
    def __init__(self, cfg, log):
        self.log = log
        self.client = SciCatClient(cfg.sc, mlzlog.getLogger("client"))
        self.workqueue = RmqClient(cfg.rmq, mlzlog.getLogger("workqueue"))

    async def setup(self):
        await self.workqueue.setup()

    async def on_message(self, msg_raw, message_context):
        try:
            await self._on_message(msg_raw, message_context)
        except Exception as e:
            self.log.exception("Uncaught error in on_message: %r", e)
            raise

    async def _on_message(self, msg_raw, message_context):
        self.log.info("processing message:")
        msg: Message = parse_message(msg_raw, self.log)
        self.log.info("message id is %s of type %s", msg.id, msg.event)
        # TODO: change msg w/ hierarchy

        dataset = msg.get_dataset_dto()
        dataset.scientificMetadata = self.get_scientific_metadata(msg)
        await self._upload(dataset, msg.get_datablock_dto(), msg.get_proposal_dto(),
                           msg.get_datafiles(), msg.get_sample_dto())
        await self._queue_filecopies(msg)
        self.log.info("done with message %s", msg.id)

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
        return unit.replace("A-1", "angstrom^-1")  # .replace("K", "kelvin")

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
        while True:
            datablock_from_scicat = await self.client.get_origdatablocks_for_dataset(dataset.pid)
            if datablock_from_scicat and len(datablock_from_scicat[-1].dataFileList) < 200:
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
            try:
                await self.client.update_datablock(datablock_from_scicat._id, update,
                                                   since=datablock_from_scicat.updatedAt)
            except PrecondFailed:
                time.sleep(0.05)
                continue
            break
        if new:
            self.log.info("Added %d new files (%db)", new, newsize - datablock.size)
        else:
            self.log.info("No new files to add.")

    async def _queue_filecopies(self, msg):
        """Queue files for copying.

        When there are multiple OrigDataBlocks we have to find out where the
        files live first. This could be optimised in the future.
        """
        # only handle completed points and scans, as the files should be finished
        if msg.event not in (Event.POINT, Event.ENDSCAN,
                             Event.ENDSUBSCAN, Event.ENDBLOCK):
            return
        datablocks = await self.client.get_origdatablocks_for_dataset(msg.dataset_pid)
        if datablocks is None or not msg.fileinfos:
            # no files in dataset yet or we don't have anything to add
            return
        samplepath = msg.fileinfos[0].samplepath
        filepaths = [str(file.path.relative_to(file.samplepath)) for file in msg.fileinfos]
        fs = [
            (datablock._id, str(samplepath / file.path))
            for datablock in datablocks[-2:]
            for file in datablock.dataFileList
            if file.path in filepaths
        ]
        await self.workqueue.queue_copies(msg.dataset_pid, fs)

    async def close(self):
        await self.client.close()
        await self.workqueue.close()
