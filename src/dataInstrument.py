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
class InstrumentFields:
    name: str = ""

class InstrumentDataset:
    def __init__(self):
        self.fields = InstrumentFields()

    def _toJson(self):
        return asdict(self.fields)

