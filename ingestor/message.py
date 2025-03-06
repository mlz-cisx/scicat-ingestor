import json
from collections import namedtuple
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import mlzlog


class Event(StrEnum):
    POINT = 'point'
    UNKNOWN = 'unknown'
    SCAN = 'scan'
    SUBSCAN = 'subscan'
    BLOCK = 'block'
    ENDSCAN = 'scan.end'
    ENDSUBSCAN = 'subscan.end'
    ENDBLOCK = 'block.end'


@dataclass(frozen=True)
class File:
    name: str
    path: str
    dataroot: str
    proposalpath: str
    samplepath: str
    size: int
    atime: str
    mtime: str
    ctime: str
    mode: int
    uid: int
    gid: int
    inode: int


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


Stats = namedtuple('Stats', ['mean', 'stddev', 'min', 'max'])


class Message:
    def __init__(self, content, log):
        # TODO: more fault tolerant, check version

        self.version = content["version"]
        self.id = content["id"]
        self.blockid = content["blockid"] if content["blockid"] != "None" else None
        self.scanid = content["scanid"] if content["scanid"] != "None" else None
        self.name = content["name"]
        self.event = Event(content["event"])
        self.creation_timestamp = content["creation_timestamp"]
        files = []
        for file in content["fileinfos"]:
            files.append(File(**file["stat"], **{k: file[k] for k in file if k != "stat"}))
        self.fileinfos = files
        mainproposer = User(
            name=content["propinfo"]["mainproposer"]["name"],
            email=content["propinfo"]["mainproposer"]["email"],
            affiliation=content["propinfo"]["mainproposer"].get("affilation"),
        )
        if "affiliation" in content["propinfo"]["mainproposer"]:
            raise ValueError("affilation fixed, remove workaround")
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


MESSAGES = {
    "v1alpha1": Message,
}


def parse_message(raw, log):
    try:
        content = json.loads(raw.decode("utf-8"))
    except UnicodeDecodeError:
        log.error("Could not decode RabbitMQ message!")
        return None
    except json.JSONDecodeError as e:
        log.error("Could not parse RabbitMQ message as json: %r", e)
        return None
    msg_version = content.get("version")
    msg_class = MESSAGES.get(msg_version)
    if msg_class is None:
        log.error("Unknown message version %r! (should be one of: )",
                  msg_version, ', '.join(MESSAGES.keys()))
        return None
    return msg_class(content, log)
