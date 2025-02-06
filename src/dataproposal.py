from dataclasses import asdict, dataclass


@dataclass
class ProposalDataset:
    ownerGroup: str = ""
    proposalId: str = "prop"
    title: str = ""
    email: str = ""
    abstract: str = ""
    # startTime: str = ""
    # endTime: str = ""

    def _toJson(self):
        return asdict(self)

# why???
# @dataclass
# class ProposalFields:
#     ownerGroup: str = ""
#     proposalId: str = "prop"
#     title: str = ""
#     email: str = ""
#     # startTime: str = ""
#     # endTime: str = ""
#
#
# class ProposalDataset:
#     def __init__(self):
#         self.fields = ProposalFields()
#
#     def _toJson(self):
#         return asdict(self.fields)
