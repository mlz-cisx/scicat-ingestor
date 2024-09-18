#!/usr/bin/env python3

# from dataclasses import asdict, dataclass


# @dataclass
# class Lifecycle:
#     archivable: bool = True
#     retrievable: bool = False
#     publishable: bool = True
#     dateOfDiskPurging: str = ""
#     archiveRetentionTime: str = ""
#     dateOfPublishing: str = ""
#     publishedOn: str = ""
#     isOnCentralDisk: bool = True
#     archiveStatusMessage: str = "datasetCreated"


# @dataclass(init=True)
# class ProposalFields:
#     ownerGroup: str = "qq"
#     proposalId: str = "ww"
#     firstname: str = "ee"
#     lastname: str = "rr"
#     title: str = "tt"
#     startTime: str = "zz"
#     endTime: str = "uu"

# class DataProposal:
#     def __init__(self):
#         self.fields = ProposalFields()

#     def _toJson(self):
#         return asdict(self.fields)

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
class ProposalFields:
    ownerGroup: str = ""
    proposalId: str = "prop"
    title: str = ""
    email: str = ""
    # startTime: str = ""
    # endTime: str = ""
    


class ProposalDataset:
    def __init__(self):
        self.fields = ProposalFields()

    def _toJson(self):
        return asdict(self.fields)

