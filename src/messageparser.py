import pprint
from datetime import datetime
from dateutil.relativedelta import relativedelta
import dataset


class MessageParser:
    VERSION = "_"

    def __init__(self, log):
        self.log = log

    def parse_message(self, message, pid_cb, instr_cb):
        if message.get("version") == self.VERSION:
            return self.make_dataset(message, pid_cb, instr_cb)
        # error on invalid?

    def make_dataset(self, data, pid_cb, instr_cb):
        raise NotImplementedError


class MessageV1Alpha1(MessageParser):
    VERSION = "v1alpha1"

    # these callbacks are not nice, do something more sensible
    # (still better than just in the ingestor)
    def make_dataset(self, msg, pid_cb, instr_cb):
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

        # self.log.debug("===== mapping =====")
        # self.log.debug(pprint.pformat(mapping))
        # self.log.debug("===== metadata =====")
        # self.log.debug(pprint.pformat(md))
        # self.log.debug("===== fileinfo =====")
        # self.log.debug(pprint.pformat(fileinfo))
        # self.log.debug("===== proposal =====")
        # self.log.debug(pprint.pformat(proposal))
        # self.log.debug("===== counterinfo =====")
        # self.log.debug(pprint.pformat(counterinfo))
        # self.log.debug("===== scaninfo =====")
        # self.log.debug(pprint.pformat(scaninfo))
        # self.log.debug("===== statistics =====")
        # self.log.debug(pprint.pformat(statistics))
        # self.log.debug("===== envdata =====")
        # self.log.debug(pprint.pformat(envdata))
        # self.log.debug(pprint.pformat(instrument))

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
        proposaluid = pid_cb(proposal)
        self.log.debug("get_proposal: %r", proposaluid)
        if proposaluid is not None:
            d.fields.proposalId = proposaluid["_id"]

        instrumentuid = instr_cb(instrument)
        if instrumentuid is not None:
            d.fields.instrumentId = instrumentuid["id"]
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

        # from statistics?
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
        return d
