import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import NamedTuple

from .client import (
    CreateDatasetDto,
    CreateOrigDatablockDto,
    CreateProposalDto,
    CreateSampleDto,
    DataFile,
)


class Event(StrEnum):
    POINT = "point"
    UNKNOWN = "unknown"
    SCAN = "scan"
    SUBSCAN = "subscan"
    BLOCK = "block"
    ENDSCAN = "scan.end"
    ENDSUBSCAN = "subscan.end"
    ENDBLOCK = "block.end"


@dataclass  # (frozen=True), would need to use setattr for frozen to be true
class File:
    name: str
    path: Path
    dataroot: str
    proposalpath: Path
    samplepath: Path
    size: int
    atime: str
    mtime: str
    ctime: str
    mode: int
    uid: int
    gid: int
    inode: int

    def __init__(
        self,
        name: str,
        path: str,
        dataroot: str,
        proposalpath: str,
        samplepath: str,
        size: int,
        atime: str,
        mtime: str,
        ctime: str,
        mode: int,
        uid: int,
        gid: int,
        inode: int,
    ):
        self.name = name
        self.path = Path(path)
        self.dataroot = Path(dataroot)
        self.proposalpath = Path(proposalpath)
        self.samplepath = Path(samplepath)
        self.size = size
        self.atime = atime
        self.mtime = mtime
        self.ctime = ctime
        self.mode = int
        self.uid = int
        self.gid = int
        self.inode = int

    def as_datafile(self):
        path = str(self.path.relative_to(self.samplepath))
        return DataFile(
            path=path,
            size=self.size,
            time=self.ctime,
            chk=None,  # will be done by the uploader
            uid=str(self.uid),
            gid=str(self.gid),
            perm=str(self.mode),
        )


@dataclass(frozen=True)
class Sample:
    name: str
    id: str


@dataclass(frozen=True)
class User:
    name: str
    email: str
    affiliation: str | None = None


@dataclass(frozen=True)
class Proposal:
    title: str
    proposal: str
    users: list[User]
    localcontacts: list[User]
    mainproposer: User
    cycle: str
    session: str
    instrument: str
    startdate: str
    enddate: str
    default_sample: str
    data_emails: list[str]
    notif_emails: list[str]
    samples: None = None  # TODO


@dataclass(frozen=True)
class Counters:
    proposal_counter: int
    sample_counter: int
    counter: int
    number: int


class Stats(NamedTuple):
    mean: float
    stddev: float
    min: float
    max: float


class Message:
    def __init__(self, content, _log):
        # TODO: more fault tolerant, check version, eval mapping

        self.version = content["version"]
        self.id = content["id"]
        self.blockid = content["blockid"] if content["blockid"] != "None" else None
        self.scanid = content["scanid"] if content["scanid"] != "None" else None
        self.name = content["name"] or ""
        self.event = Event(content["event"])
        self.dataroot = Path(content["dataroot"])
        self.proposalpath = Path(content["proposalpath"])
        self.samplepath = Path(content["samplepath"])
        self.creation_timestamp = content["creation_timestamp"]
        files = [
            File(
                dataroot=self.dataroot,
                proposalpath=self.proposalpath,
                samplepath=self.samplepath,
                **file["stat"],
                **{k: file[k] for k in file if k != "stat"},
            )
            for file in content["fileinfos"]]
        self.fileinfos = files
        mainproposer = User(**content["propinfo"]["mainproposer"])
        self.propinfo = Proposal(
            title=content["propinfo"]["title"],
            proposal=content["propinfo"]["proposal"],
            users=[User(**u) for u in content["propinfo"]["users"]],
            localcontacts=[User(**u) for u in content["propinfo"]["localcontacts"]],
            mainproposer=mainproposer,
            cycle=content["propinfo"]["cycle"],
            session=content["propinfo"]["session"],
            instrument=content["propinfo"]["instrument"],
            startdate=content["propinfo"]["startdate"],
            enddate=content["propinfo"]["enddate"],
            default_sample=content["propinfo"]["default_sample"],
            data_emails=content["propinfo"]["default_sample"],
            notif_emails=content["propinfo"]["default_sample"],
            samples=content["propinfo"]["default_sample"],
        )
        self.metadata = content["metadata"]  # TODO
        self.statistics = {
            k: Stats(**v) for k, v in content["statistics"].items()
        }
        self.counterinfo = Counters(**content["counterinfo"])
        self.canonicalvalues = content["canonicalvalues"]

        self.sample = Sample(
            name=self._get_mapped(content, "sample", "samplename"),
            id=self._get_mapped(content, "sample", "value"),
        )
        self.instrument = self._get_mapped(content, "instrument", "instrument")

    def _get_mapped(self, msg, mapping, key):
        mapped = msg["metadata"].get(msg["mapping"].get(mapping))
        ret = mapped.get(key, {}).get("value")
        if ret is None:
            raise ValueError(f"Could not resolve mapping {mapping} with key {key}!")
        return ret

    @property
    def dataset_pid(self):
        if self.event in {Event.SCAN, Event.ENDSCAN}:
            return self.id
        return self.scanid

    def get_dataset_dto(self):
        return CreateDatasetDto(
            type="raw",
            owner="ingestor",
            ownerGroup=self.propinfo.session,
            accessGroups=[self.propinfo.session, self.propinfo.instrument],
            sourceFolder=self.samplepath,
            contactEmail="datasteward@mlz-garching.de",  # TODO
            creationLocation="MLZ",  # TODO: instrument?
            principalInvestigator=self.propinfo.mainproposer.name,
            datasetName=self.name or "Scan",
            creationTime=datetime.now(tz=UTC).isoformat(),
            pid=self.dataset_pid,
            isPublished=False,
            description="not available",
            # scientificMetadata=self.get_scientific_metadata(),
            runNumber=str(0),
            sampleId=str(self.sample.id),
            instrumentId=self.instrument,
        )

    def get_proposal_dto(self):
        return CreateProposalDto(
            ownerGroup=self.propinfo.session,
            accessGroups=[self.propinfo.instrument],
            proposalId=self.propinfo.proposal,
            email=self.propinfo.mainproposer.email,
            title=self.propinfo.title,
        )

    def get_sample_dto(self):
        return CreateSampleDto(
            sampleId=str(self.sample.id),
            owner="ingestor",
            ownerGroup="ingestor",
            accessGroups=[],
            instrumentGroup="",
            description=self.sample.name,
        )

    def get_datafiles(self):
        return [fileinfo.as_datafile() for fileinfo in self.fileinfos]

    def get_datablock_dto(self):
        return CreateOrigDatablockDto(
            datasetId=self.dataset_pid,
            ownerGroup=self.propinfo.session,
            accessGroups=[self.propinfo.instrument],
            instrumentGroup=self.propinfo.instrument,
            chkAlg="sha256",
            dataFileList=self.get_datafiles(),
            size=sum(file.size for file in self.fileinfos),
        )


MESSAGES = {
    "v1alpha1": Message,
}


def parse_message(raw, log):
    try:
        content = json.loads(raw.body.decode("utf-8"))
    except UnicodeDecodeError:
        log.error("Could not decode RabbitMQ message!")  # noqa: TRY400
        return None
    except json.JSONDecodeError as e:
        log.error("Could not parse RabbitMQ message as json: %r", e)  # noqa: TRY400
        return None
    msg_version = content.get("version")
    msg_class = MESSAGES.get(msg_version)
    if msg_class is None:
        log.error("Unknown message version %r! (should be one of: %s)",
                  msg_version, ", ".join(MESSAGES.keys()))
        return None
    return msg_class(content, log)
