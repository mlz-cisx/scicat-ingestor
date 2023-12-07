#!/usr/env/bin python3

from dataclasses import dataclass, field


@dataclass
class Datafile:
    path: str
    time: str
    size: int = 0


@dataclass
class OrigDatablock:
    dataFileList: list = field(default_factory=list)
    size: int = 0


@dataclass
class Datablock:
    dataFileList: list = field(default_factory=list)
    ownerGroup: str = "mlz"
    datasetId: str = ""
    rawDatasetId: str = ""
    archiveId: str = ""
    size: int = 0
    version: str = "1"
