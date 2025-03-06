import os
from dataclasses import asdict, dataclass
from enum import StrEnum

import aiohttp


def export(c):
    return asdict(c, dict_factory=lambda d: {k: v for k, v in d if v is not None})


class AuthStrategy(StrEnum):
    FUNCTIONAL = "login"
    LDAP = "ldap"


class SciCatClient:
    def __init__(self, config, log):
        self.log = log
        self.session = aiohttp.ClientSession()
        # config
        self.config = config
        self.expires = None

    async def login(self, user, password):
        async with self.session.post(
            f"{self.config.url}api/v3/auth/{self.config.auth_strat}",
            json={"username": user, "password": password},
        ) as response:
            if 200 <= response.status < 300:
                body = await response.json()
                if (token := body.get('access_token')) is not None:
                    self.session.headers["Authorization"] = f"Bearer {token}"
                # TODO: manage lifetime of token etc
                # self.expires
                # body.get('created')
                # body.get('expires_in')
                # body.get('ttl')

    async def get(self, path, **kwds):
        async with self.session.get(
            f"{self.config.url}{path}",
            **kwds
        ) as response:
            if 200 <= response.status < 300:
                return await response.json()
            if response.status >= 400:
                self.log.error("Error in get request: %r", await response.text)

    async def post(self, path, **kwds):
        async with self.session.post(
            f"{self.config.url}{path}",
            **kwds
        ) as response:
            if 200 <= response.status < 300:
                return await response.json()
            if response.status >= 400:
                self.log.error("Error in post request: %r", await response.text)

    async def close(self):
        await self.session.close()

# Alternative 1:
# class DS:
#     title = Field(post=True, patch=True, get=True)
#     createdAt = Field(post=False, patch=False, get=True)
# Alternative 2:
# openapi generated?


@dataclass
class DatasetLifeCycle:
    archivable: bool | None = None
    retrievable: bool | None = None
    publishable: bool | None = None
    dateOfDiskPurging: str | None = None
    archiveRetentionTime: str | None = None
    dateOfPublishing: str | None = None
    publishedOn: str | None = None
    isOnCentralDisk: bool | None = None
    archiveStatusMessage: str | None = None
    retrieveStatusMessage: str | None = None
    archiveReturnMessage: dict | None = None
    retrieveReturnMessage: dict | None = None
    exportedTo: str | None = None
    retrieveIntegrityCheck: bool | None = None


@dataclass
class PostDataset:
    type: str
    ownerGroup: str
    contactEmail: str
    sourceFolder: str
    datasetName: str
    creationTime: str
    pid: str  # original schema has this as optional, but we always want it
    owner: str
    creationLocation: str  # schema says optional, internal error says no
    accessGroups: list[str] | None = None
    instrumentGroup: str | None = None
    ownerEmail: str | None = None
    orcidOfOwner: str | None = None
    sourceFolderHost: str | None = None
    size: int | None = None
    packedSize: int | None = None
    numberOfFiles: int | None = None
    numberOfFilesArchived: int | None = None
    validationStatus: str | None = None
    keywords: list[str] | None = None
    description: str | None = None
    classification: str | None = None
    license: str | None = None
    isPublished: bool = False
    # techniques: list[Techniques] | None
    sharedWith: list[str] | None = None
    # relationships: list[Relationship]
    datasetlifecycle: DatasetLifeCycle | None = None
    scientificMetadata: dict | None = None
    comment: str | None = None
    dataQualityMetrics: int | None = None
    principalInvestigator: str | None = None
    startTime: str | None = None
    endTime: str | None = None
    dataFormat: str | None = None
    proposalIds: list[str] | None = None
    sampleIds: list[str] | None = None
    instrumentIds: list[str] | None = None
    inputDatasets: list[str] | None = None
    usedSoftware: list[str] | None = None
    # jobParameters: ??? dict?
    jobLogData: str | None = None
    runNumber: str | None = None


@dataclass
class Dataset:
    createdBy: str
    updatedBy: str
    createdAt: str
    updatedAt: str
    ownerGroup: str
    isPublished: bool
    pid: str
    owner: str
    contactEmail: str
    sourceFolder: str
    size: int
    numberOfFiles: int
    creationTime: str
    type: str
    creationLocation: str
    datasetName: str
    version: str
    accessGroups: list[str]
    datasetlifecycle: DatasetLifeCycle
    instrumentGroup: str | None = None
    ownerEmail: str | None = None
    orcidOfOwner: str | None = None
    sourceFolderHost: str | None = None
    packedSize: int | None = None
    numberOfFilesArchived: int | None = None
    validationStatus: str | None = None
    keywords: list[str] | None = None
    description: str | None = None
    classification: str | None = None
    license: str | None = None
    # history: list[History] | None = None
    # techniques: list[Technique] | None = None
    # relationships: list[Relationship] | None = None
    sharedWith: list[str] | None = None
    scientificMetadata: dict | None = None
    comment: str | None = None
    dataQualityMetrics: int | None = None
    principalInvestigator: str | None = None
    startTime: str | None = None
    endTime: str | None = None
    dataFormat: str | None = None
    runNumber: str | None = None
    proposalIds: list[str] | None = None
    sampleIds: list[str] | None = None
    instrumentIds: list[str] | None = None
    inputDatasets: list[str] | None = None
    usedSoftware: list[str] | None = None
    # jobParameters: ???
    jobLogData: str | None = None


@dataclass
class DataFile:
    path: str
    size: int
    time: str
    chk: str | None = None
    uid: str | None = None
    gid: str | None = None
    perm: str | None = None


@dataclass
class PostOrigDataBlock:
    datasetId: str
    size: int
    dataFilelist: list[DataFile]
    ownerGroup: str | None = None
    accessGroups: list[str] | None = None
    instrumentGroup: str | None = None
    chkAlg: str | None = None


@dataclass
class OrigDataBlock:
    ownerGroup: str
    instrumentGroup: str
    accessGroups: list[str]
    datasetId: str
    size: int
    dataFilelist: list[DataFile]
    _id: str
    createdBy: str
    updatedBy: str
    createdAt: str
    updatedAt: str
    isPublished: bool = False
    chkAlg: str | None = None


@dataclass
class PostSample:
    ownerGroup: str
    accessGroups: list[str] | None = None
    instrumentGroup: str | None = None
    sampleId: str | None = None
    owner: str | None = None
    description: str | None = None
    sampleCharacteristics: dict[str, object] | None = None
    isPublished: bool = False


@dataclass
class Sample:
    createdBy: str
    updatedBy: str
    createdAt: str
    updatedAt: str
    ownerGroup: str
    accessGroups: list[str]
    sampleId: str
    isPublished: bool = False
    instrumentGroup: str | None = None
    owner: str | None = None
    description: str | None = None
    sampleCharacteristics: dict[str, object] | None = None


@dataclass
class MeasurementPeriod:
    instrument: str
    start: str
    end: str
    createdBy: str | None = None
    updatedBy: str | None = None
    createdAt: str | None = None
    updatedAt: str | None = None
    comment: str | None = None


@dataclass
class PostProposal:
    ownerGroup: str
    proposalId: str
    email: str
    title: str
    accessGroups: list[str] | None = None
    instrumentGroup: str | None = None
    pi_email: str | None = None
    pi_firstname: str | None = None
    pi_lastname: str | None = None
    firstname: str | None = None
    lastname: str | None = None
    abstract: str | None = None
    startTime: str | None = None
    endTime: str | None = None
    MeasurementPeriodlist: list[MeasurementPeriod] | None = None
    metadata: dict[str, object] | None = None
    parentProposalId: str | None = None
    type: str | None = None


@dataclass
class Proposal:
    ownerGroup: str
    proposalId: str
    email: str
    title: str
    createdBy: str
    updatedBy: str
    createdAt: str
    updatedAt: str
    accessGroups: list[str]
    parentProposalId: str
    type: str
    instrumentGroup: str | None = None
    isPublished: bool = False
    pi_email: str | None = None
    pi_firstname: str | None = None
    pi_lastname: str | None = None
    firstname: str | None = None
    lastname: str | None = None
    abstract: str | None = None
    startTime: str | None = None
    endTime: str | None = None
    MeasurementPeriodlist: list[MeasurementPeriod] | None = None
    metadata: dict[str, object] | None = None
