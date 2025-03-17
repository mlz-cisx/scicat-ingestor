import os
import json
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timedelta
from enum import StrEnum

import aiohttp
from dacite import from_dict


def to_dict(c):
    return asdict(c, dict_factory=lambda d: {k: v for k, v in d if v is not None})


def from_response(response_type, data, multiple=False):
    if not data:
        # if there are no results, some endpoints don't give an empty list, but
        # an empty body
        return None
    if not is_dataclass(response_type):
        raise ValueError("response_type has to be a dataclass (got %r)" % response_type)
    if multiple:
        if isinstance(data, list):
            return [from_dict(response_type, elem) for elem in data]
        else:
            print(data)
            raise ValueError("Did not get a list as data for multiple=True")
    return from_dict(response_type, data)


class AuthStrategy(StrEnum):
    FUNCTIONAL = "login"
    LDAP = "ldap"


class SciCatClient:
    def __init__(self, config, log):
        self.log = log
        self.session = aiohttp.ClientSession()
        self.config = config
        self.expires = datetime.now()

    async def _get_endpoint(self, endpoint, response_type, *args, multiple=False, **kwargs):
        self.log.debug('GET %s expecting %s (multiple=%s)',
                       endpoint, response_type.__name__, multiple)
        async with self.session.get(
            self.config.url + endpoint, *args, **kwargs,
        ) as response:
            if 200 <= response.status < 300:
                try:
                    return from_response(response_type, await response.json(), multiple)
                except TypeError as e:
                    self.log.error("Error in get request to %s: %r with %s", endpoint, e, await response.json())
            if response.status >= 400:
                self.log.error("Error in post request to %s: %r", endpoint, await response.text())

    async def _post_endpoint(self, endpoint, response_type, request_data, *args, **kwargs):
        self.log.debug('POST %s with %s expecting %s',
                       endpoint, request_data, response_type.__name__,)
        async with self.session.post(
            self.config.url + endpoint, *args, json=to_dict(request_data), **kwargs,
        ) as response:
            if 200 <= response.status < 300:
                return from_response(response_type, await response.json())
            if response.status >= 400:
                self.log.error("Error in post request to %s: %r", endpoint, await response.text())

    async def _patch_endpoint(self, endpoint, response_type, request_data, *args, **kwargs):
        self.log.debug('PATCH %s with %s expecting %s',
                       endpoint, request_data, response_type.__name__,)
        async with self.session.patch(
            self.config.url + endpoint, *args, json=to_dict(request_data), **kwargs,
        ) as response:
            if 200 <= response.status < 300:
                return from_response(response_type, await response.json())
            if response.status >= 400:
                self.log.error("Error in post request to %s: %r", endpoint, await response.text())

    async def _search(self, endpoint, response_type, filter=None):
        return await self._get_endpoint(endpoint, response_type, multiple=True,
                                        params={"filter": json.dumps(filter)})

    async def login(self, user, password):
        login_info = await self._post_endpoint(
            f"/api/v3/auth/{self.config.auth_strat}",
            AuthResponse,
            CredentialsDto(user, password),
        )
        self.session.headers["Authorization"] = f"Bearer {login_info.access_token}"
        self.expires = datetime.fromisoformat(login_info.created) \
            + timedelta(seconds=login_info.ttl)
        self.log.debug("Got Auth token. Expires on %s.", self.expires)

    def ensure_logged_in(f):
        async def inner(self, *args, **kwargs):
            if self.expires.timestamp() < datetime.now().timestamp():
                # we have to refresh the token
                self.log.debug("Initial authentication required or previous "
                               "is expired, logging in")
                await self.login(self.config.user, self.config.pw)
            return await f(self, *args, **kwargs)
        return inner

    @ensure_logged_in
    async def create_dataset(self, dataset):
        return await self._post_endpoint(
            "/api/v3/datasets",
            Dataset,
            dataset,
        )

    @ensure_logged_in
    async def get_dataset_by_pid(self, pid):
        return await self._get_endpoint(
            f"/api/v3/datasets/{pid}",
            Dataset,
        )

    @ensure_logged_in
    async def get_origdatablocks_for_dataset(self, pid):
        return await self._get_endpoint(
            f"/api/v3/datasets/{pid}/origdatablocks",
            OrigDataBlock,
            multiple=True,
        )

    @ensure_logged_in
    async def create_origdatablock(self, datablock):
        return await self._post_endpoint(
            "/api/v3/origdatablocks",
            OrigDataBlock,
            datablock,
        )

    @ensure_logged_in
    async def update_datablock(self, datablock_id, datablock_update):
        return await self._patch_endpoint(
            f"/api/v3/origdatablocks/{datablock_id}",
            OrigDataBlock,
            datablock_update,
        )


    @ensure_logged_in
    async def create_proposal(self, proposal):
        return await self._post_endpoint(
            "/api/v3/proposals",
            Proposal,
            proposal,
        )

    @ensure_logged_in
    async def get_proposal_by_pid(self, pid):
        return await self._get_endpoint(
            f"/api/v3/proposals/{pid}",
            Proposal,
        )

    async def get_or_create_proposal(self, proposal):
        present = await self.get_proposal_by_pid(proposal.proposalId)
        if present is not None:
            self.log.info("Proposal %s found", proposal.proposalId)
            return present
        self.log.info("Proposal %s not found in SciCat, creating.", proposal.proposalId)
        return await self.create_proposal(proposal)

    @ensure_logged_in
    async def get_instrument_by_pid(self, id):
        return await self._get_endpoint(
            f"/api/v3/instruments/{id}",
            Instrument,
        )

    @ensure_logged_in
    async def search_instruments(self, filter=None):
        return await self._search(
            "/api/v3/instruments",
            Instrument,
            filter,
        )

    @ensure_logged_in
    async def instrument_by_unique_name(self, uname):
        return await self.search_instruments(filter={"where": {"uniqueName": uname}})

    @ensure_logged_in
    async def create_sample(self, sample):
        return await self._post_endpoint(
            "/api/v3/sample",
            Sample,
            sample,
        )

    @ensure_logged_in
    async def get_sample_by_id(self, sampleId):
        return await self._get_endpoint(
            f"/api/v3/samples/{sampleId}",
            Sample,
        )

    async def close(self):
        await self.session.close()


@dataclass
class CredentialsDto:
    username: str
    password: str

    def __str__(self):
        # hide password in str formating for debug log
        return f"CredentialsDto(username={self.username}, password=\"***\")"


@dataclass
class ReturnedUserDto:
    id: str
    username: str
    email: str
    authStrategy: str
    realm: str | None = None
    email_verified: bool | None = None


@dataclass
class AuthResponse:
    access_token: str
    id: str
    expires_in: str | float
    ttl: str | float
    created: str
    userId: str
    user: ReturnedUserDto


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
    archiveReturnMessage: dict[str, object] | None = None
    retrieveReturnMessage: dict[str, object] | None = None
    exportedTo: str | None = None
    retrieveIntegrityCheck: bool | None = None


@dataclass
class CreateDatasetDto:
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
    scientificMetadata: dict[str, object] | None = None
    comment: str | None = None
    dataQualityMetrics: int | None = None
    principalInvestigator: str | None = None
    startTime: str | None = None
    endTime: str | None = None
    dataFormat: str | None = None
    proposalId: str | None = None
    sampleId: str | None = None
    instrumentId: str | None = None
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
    scientificMetadata: dict[str, object] | None = None
    comment: str | None = None
    dataQualityMetrics: int | None = None
    principalInvestigator: str | None = None
    startTime: str | None = None
    endTime: str | None = None
    dataFormat: str | None = None
    runNumber: str | None = None
    proposalIds: list[str] | None | list = None
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
class CreateOrigDatablockDto:
    datasetId: str
    size: int
    dataFileList: list[DataFile]
    ownerGroup: str | None = None
    accessGroups: list[str] | None = None
    instrumentGroup: str | None = None
    chkAlg: str | None = None


@dataclass
class OrigDataBlock:
    ownerGroup: str
    accessGroups: list[str]
    datasetId: str
    size: int
    dataFileList: list[DataFile]
    _id: str
    createdBy: str
    updatedBy: str
    createdAt: str
    updatedAt: str
    instrumentGroup: str | None = None
    isPublished: bool = False
    chkAlg: str | None = None


@dataclass
class UpdateOrigDataBlockDto:
    size: float
    dataFileList: list[DataFile]
    owner_group: str | None = None
    access_groups: list[str] | None = None
    instrument_group: str | None = None
    chk_alg: str | None = None


@dataclass
class CreateSampleDto:
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
class CreateProposalDto:
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
    type: str
    parentProposalId: str | None = None
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


@dataclass
class Instrument:
    pid: str
    uniqueName: str
    name: str
    customMetadata: dict[str, object]
