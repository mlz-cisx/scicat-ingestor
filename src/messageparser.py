import pprint
import math
import textwrap
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import dataset
import dataproposal


class MessageParser:
    VERSION = "_"

    def __init__(self, log):
        self.log = log

    def parse_message(self, message, pid_cb, instr_cb, prop_cb, sample_cb):
        if message.get("version") == self.VERSION:
            return self.make_dataset(message, pid_cb, instr_cb, prop_cb, sample_cb)
        # error on invalid?

    def make_dataset(self, data, pid_cb, instr_cb, prop_cb, sample_cb):
        raise NotImplementedError


class MessageV1Alpha1(MessageParser):
    VERSION = "v1alpha1"

    # these callbacks are not nice, do something more sensible
    # (still better than just in the ingestor)
    def make_dataset(self, msg, pid_cb, instr_cb, prop_cb, sample_cb):
        md = msg["metadata"]
        mapping = msg["mapping"]
        exp = mapping["experiment"]
        sample = mapping["sample"]
        fileinfo = msg.get("fileinfos", [])
        pinfo = msg["propinfo"]
        # TODO: sampletracker info in NICOS, multiple samples w/ ID
        proposal = pinfo["proposal"]
        counterinfo = msg.get("counterinfo", {})
        scaninfo = msg.get("scaninfo", {})
        statistics = msg.get("statistics", {})
        instrument = pinfo["instrument"]

        # TODO: currently constant settings
        owner = "ingestor"
        ownerGroup = "rabbitmq"
        accessGroups = [pinfo["session"], proposal, instrument]

        now = datetime.now()
        np3y = now + relativedelta(years=3)
        np10y = now + relativedelta(years=10)

        d = dataset.Dataset()
        d.pid = msg["id"]
        d.datasetName = msg["name"] or "Scan " + str(counterinfo.get("counter", -1))
        if scaninfo:
            d.description = scaninfo[d.id]
        else:
            d.description = "not available"
        d.creationLocation = "MLZ"
        d.owner = owner
        d.principalInvestigator = pinfo["users"][0]["name"]
        # TODO: datasteward or PI email?
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
        d.ownerGroup = ownerGroup
        d.proposalId = proposal
        d.accessGroups = accessGroups
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
            # TODO: PI vs. Main Proposer (frontend)?
            p.email = pinfo["mainproposer"]["email"]
            p.title = md[exp]["title"]["value"]
            p.ownerGroup = proposal
            p.accessGroups = "rabbitmq"
            p.abstract = textwrap.dedent("""
            Lorem ipsum odor amet, consectetuer adipiscing elit. Primis consequat taciti tincidunt urna blandit tincidunt facilisi mattis. Mus taciti est; pretium lectus fermentum risus. Rutrum aliquet diam habitant ante diam maecenas diam mus. At eget cursus aliquet tincidunt quam libero duis. Pulvinar dui primis donec rutrum aliquam. Porttitor tincidunt accumsan conubia eleifend urna laoreet.

            Potenti ante potenti diam scelerisque bibendum mi euismod. Faucibus purus ipsum porta auctor mattis congue integer. Tincidunt a mus sit quam diam dignissim urna in. Lacinia ultrices placerat sit erat lacus. Bibendum accumsan pharetra himenaeos condimentum ut iaculis. Potenti rhoncus morbi platea adipiscing morbi tortor nunc finibus. Maecenas tincidunt fames parturient ut, platea fusce tincidunt. Amet sit conubia vel fusce ipsum varius sagittis. Quis ac volutpat vitae litora senectus finibus maximus euismod.
            """)

            j = p._toJson()
            self.log.debug(pprint.pformat(j))
            d.proposalId = prop_cb("api/v3/proposals", json=j).json()["id"]

        instrumentuid = instr_cb(f"MLZ.{instrument}")  # TODO
        if instrumentuid is not None:
            d.instrumentId = instrumentuid["id"]
        sid = str(md[sample]["sampleid"]["value"])
        sample_id = sample_cb(sid)
        if not sample_id:
            # TODO: instrumentGroup, sampleCharacteristics
            sampleobj = {
                "ownerGroup": ownerGroup,
                "accessGroups": accessGroups,
                "sampleId": sid,
                "owner": owner,
                "description": md[sample]["samplename"]["value"],
                "sampleCharacteristics": {},
                "isPublished": False,
            }
            # TODO: currently misuse prob_cb for post request
            sample_id = prop_cb("api/v3/samples", json=sampleobj).json()["id"]

        d.sampleId = sample_id
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
