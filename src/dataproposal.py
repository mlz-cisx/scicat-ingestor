from dataclasses import asdict, dataclass


@dataclass
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
