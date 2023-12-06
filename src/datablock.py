#!/usr/env/bin python3

from dataclasses import asdict, dataclass, field


@dataclass
class datafile:
    path: str
    time: str
    size: int = 0


@dataclass
class origdatablock:
    dataFileList: list = field(default_factory=list)
    size: int = 0


@dataclass
class datablock:
    dataFileList: list = field(default_factory=list)
    ownerGroup: str = "mlz"
    datasetId: str = ""
    rawDatasetId: str = ""
    archiveId: str = ""
    size: int = 0
    version: str = "1"
