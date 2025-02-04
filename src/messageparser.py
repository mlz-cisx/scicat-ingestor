import pprint
import math
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import dataset
import dataproposal


class MessageParser:
    VERSION = "_"

    def __init__(self, log):
        self.log = log

    def parse_message(self, message, pid_cb, instr_cb, prop_cb):
        if message.get("version") == self.VERSION:
            return self.make_dataset(message, pid_cb, instr_cb, prop_cb)
        # error on invalid?

    def make_dataset(self, data, pid_cb, instr_cb, prop_cb):
        raise NotImplementedError


class MessageV1Alpha1(MessageParser):
    VERSION = "v1alpha1"

    # these callbacks are not nice, do something more sensible
    # (still better than just in the ingestor)
    def make_dataset(self, msg, pid_cb, instr_cb, prop_cb):
        md = msg["metadata"]
        mapping = msg["mapping"]
        exp = mapping["experiment"]
        sample = mapping["sample"]
        fileinfo = msg.get("fileinfos", [])
        proposal = md[exp]["proposal"]["value"]
        counterinfo = msg.get("counterinfo", {})
        scaninfo = msg.get("scaninfo", {})
        statistics = msg.get("statistics", {})
        instrument = mapping["instrument"].upper()

        now = datetime.now()
        np3y = now + relativedelta(years=3)
        np10y = now + relativedelta(years=10)

        d = dataset.Dataset()
        d.pid = msg["id"]
        d.datasetName = md[exp]["title"]["value"]
        if scaninfo:
            d.description = scaninfo[d.id]
        else:

            d.description = "not available"
        # d.datasetName = msg['metadata']['Exp']['title']
        d.creationLocation = "MLZ"
        d.owner = "ingestor"
        d.principalInvestigator = md[exp]["users"]["value"]
        d.contactEmail = "datasteward@frm2.tum.de"
        d.ownerEmail = "datasteward@frm2.tum.de"
        if fileinfo:
            dataroot = Path(fileinfo[0].get("dataroot", "/"))
            samplepath = Path(fileinfo[0].get("samplepath", "/"))
            d.sourceFolder = str("/nfs" / samplepath.relative_to(dataroot))
        else:
            d.sourceFolder = "/"

        d.creationTime = now.isoformat()
        d.endTime = now.isoformat()
        d.type = "raw"
        d.ownerGroup = "rabbitmq"
        d.proposalId = proposal
        d.accessGroups = ["E00001-01A", proposal, instrument.lower()]
        d.datasetlifecycle.dateOfDiskPurging = np10y.isoformat()
        d.datasetlifecycle.archiveRetentionTime = np10y.isoformat()
        d.datasetlifecycle.dateOfPublishing = np3y.isoformat()
        d.isPublished = False
        # d.ownerGroup = proposal or "scicat"
        proposaluid = pid_cb(proposal)
        self.log.debug("get_proposal: %r", proposaluid)
        if proposaluid is not None:
            d.proposalId = proposaluid["_id"]
        else:
            p = dataproposal.ProposalDataset()
            p.proposalId = proposal
            p.email = "email@tum.com"
            p.title = "Cookies"
            p.ownerGroup = proposal
            p.accessGroups = "rabbitmq"

            j = p._toJson()
            self.log.debug(pprint.pformat(j))
            d.proposalId = prop_cb("api/v3/proposals", json=j).json()["id"]

        print(f"===> {instrument}")
        instrumentuid = instr_cb(f"MLZ.{instrument}")  # TODO
        if instrumentuid is not None:
            d.instrumentId = instrumentuid["id"]
        d.sampleId = str(md[sample]["sampleid"]["value"])

        # from statistics?
        sdm = {}
        for dev, stats in statistics.items():
            unit = md.get(dev, {}).get("unit", "").replace("A-1", "angstrom^-1")
            if stats.get("stddev") == 0.0 or math.isinf(stats.get("stddev") or 0):  # TODO
                sdm[dev] = {
                    "value": stats.get("mean", "-"),
                    "unit": unit,
                }
                continue
            for key in ("mean", "stddev", "min", "max"):
                sdm[f"{dev}_{key}"] = {
                    "value": stats.get(key, "-"),
                    "unit": unit,
                }

        for dev, dev_params in md.items():
            if dev in statistics:
                continue
            for param, parammd in dev_params.items():
                if not parammd:
                    continue
                val = parammd.get("value")
                if val is None:
                    continue
                unit = parammd.get("unit", "").replace("A-1", "angstrom^-1")
                cat_name = f"{dev}_{param}" if param != "value" else dev
                if isinstance(val, list):
                    if len(val) > 1 and isinstance(val[1], str) and "," in val[1]:
                        sdm[cat_name] = {"value": f"{val[0]} {val[1]}", "unit": unit}
                    elif isinstance(val[0], (int, float)):
                        sdm[cat_name] = {"value": val[0], "unit": unit}
                    else:
                        # handle mixed type devices that return a str for some settings
                        sdm[cat_name] = {"value": str(val[0]), "unit":  ''}
                elif isinstance(val, str):
                    sdm[cat_name] = {"value": val, "unit": ""}
                else:
                    sdm[cat_name] = {"value": val, "unit": unit}

        sdm["runNumber"] = {"value": counterinfo.get("counter", -1), "unit": ""}

        d.scientificMetadata = sdm
        self.log.debug(pprint.pformat(sdm))
        return d
