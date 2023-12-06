#!/usr/bin/env python3

from dataclasses import asdict, dataclass, field


@dataclass
class Lifecycle:
    archivable: bool = True
    retrievable: bool = False
    publishable: bool = True
    dateOfDiskPurging: str = ""
    archiveRetentionTime: str = ""
    dateOfPublishing: str = ""
    publishedOn: str = ""
    isOnCentralDisk: bool = True
    archiveStatusMessage: str = "datasetCreated"


@dataclass(init=True)
class DatasetFields:
    datasetName: str = ""
    principalInvestigator: str = ""
    endTime: str = ""
    creationLocation: str = ""
    dataFormat: str = ""
    scientificMetadata: dict = field(default_factory=dict)
    owner: str = ""
    ownerEmail: str = ""
    contactEmail: str = ""
    sourceFolder: str = ""
    size: int = 0
    packedSize: int = 0
    creationTime: str = ""
    type: str = ""
    validationStatus: str = ""
    keywords: list = field(default_factory=list)
    description: str = ""
    datasetName: str = ""
    classification: str = "AV=medium,CO=low"
    license: str = "CC-BY-4.0"
    version: str = ""
    isPublished: bool = False,
    ownerGroup: str = "mlz"
    accessGroups: list = field(default_factory=list)
    proposalId: str = ""
    sampleId: str = "ad2YBhhyw"
    datasetlifecycle: Lifecycle = field(default_factory=Lifecycle)


class Dataset:
    def __init__(self):
        self.fields = DatasetFields()

    def _toJson(self):
        return asdict(self.fields)
